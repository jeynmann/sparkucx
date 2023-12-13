package org.apache.spark.shuffle.ucx

// import org.apache.spark.SparkEnv
import org.apache.spark.shuffle.utils.{UcxLogging, UnsafeUtils}
import org.apache.spark.shuffle.ucx.utils.SerializationUtils
import org.apache.spark.network.shuffle.ExternalUcxShuffleBlockResolver
// import org.apache.spark.util.ThreadUtils
// import scala.concurrent.forkjoin.{ForkJoinPool => SForkJoinPool, ForkJoinWorkerThread => SForkJoinWorkerThread}
import java.util.concurrent.{ForkJoinPool, ForkJoinWorkerThread}
import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{Channels, ReadableByteChannel}
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class UcxShuffleTransportServer(
  serverConf: ExternalUcxServerConf, blockManager: ExternalUcxShuffleBlockResolver)
  extends ExternalShuffleTransport(serverConf)
  with UcxLogging {
  private[ucx] val workerMap = new TrieMap[String, TrieMap[Long, ByteBuffer]]
  private val errorHandler = new UcpEndpointErrorHandler {
    override def onError(ucpEndpoint: UcpEndpoint, errorCode: Int, errorString: String): Unit = {
      if (errorCode == UcsConstants.STATUS.UCS_ERR_CONNECTION_RESET) {
        logWarning(s"Connection closed on ep: $ucpEndpoint")
      } else {
        logError(s"Ep $ucpEndpoint got an error: $errorString")
      }
      endpoints.remove(ucpEndpoint)
      ucpEndpoint.close()
    }
  }
  // private val factory = new SForkJoinPool.ForkJoinWorkerThreadFactory {
  //   override def newThread(pool: SForkJoinPool) =
  //     new SForkJoinWorkerThread(pool) {
  //       setName(s"UCX-listener-${super.getName}")
  //     }
  // }
  // private val replyExecutors = new SForkJoinPool(
  //   serverConf.numListenerThreads, factory, null, false)
  private val factory = new ForkJoinPool.ForkJoinWorkerThreadFactory {
    override def newThread(pool: ForkJoinPool) =
      new ForkJoinWorkerThread(pool) {
        setName(s"UCX-listener-${super.getName}")
      }
  }
  private val replyExecutors = new ForkJoinPool(
    serverConf.numListenerThreads, factory, null, false)

  private val endpoints = mutable.Set.empty[UcpEndpoint]
  private var globalWorker: UcpWorker = _
  private var listener: UcpListener = _

  private[this] class ProgressTask(worker: UcpWorker) extends Runnable {
    override def run(): Unit = {
      val useWakeup = ucxShuffleConf.useWakeup
      while (running) {
        try {
          worker.synchronized {
            while (worker.progress != 0) {}
          }
          if (useWakeup) {
            worker.waitForEvents()
          }
        } catch {
          case e: Throwable => logError(s"Exception in progress:${e}")
        }
      }
    }
  }

  override def estimateNumEps(): Int = serverConf.ucxEpsNum

  override def init(): ByteBuffer = {
    initContext()
    initMemoryPool()

    if (serverConf.useWakeup) {
      ucpWorkerParams.requestWakeupRX().requestWakeupTX().requestWakeupEdge()
    }

    logInfo(s"Allocating global workers")
    globalWorker = ucxContext.newWorker(ucpWorkerParams)
    listener = globalWorker.newListener(new UcpListenerParams().setSockAddr(
      new InetSocketAddress("0.0.0.0", serverConf.ucxServerPort))
      .setConnectionHandler((ucpConnectionRequest: UcpConnectionRequest) => {
        endpoints.add(globalWorker.newEndpoint(new UcpEndpointParams().setConnectionRequest(ucpConnectionRequest)
          .setPeerErrorHandlingMode().setErrorHandler(errorHandler)
          .setName(s"Endpoint to ${ucpConnectionRequest.getClientId}")))
      }))
    // Main RPC thread. Submit each RPC request to separate thread and send reply back from separate worker.
    globalWorker.setAmRecvHandler(0,
      (headerAddress: Long, headerSize: Long, amData: UcpAmData, _: UcpEndpoint) => {
      val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
      val workerId = UcxWorkerId.deserialize(header)
      val replyTag = header.getInt
      val exeId = header.getInt
      handleFetchBlockRequest(workerId, exeId, replyTag, amData)
      UcsConstants.STATUS.UCS_INPROGRESS
    }, UcpConstants.UCP_AM_FLAG_PERSISTENT_DATA | UcpConstants.UCP_AM_FLAG_WHOLE_MSG )
    // AM to get worker address for client worker and connect server workers to it
    globalWorker.setAmRecvHandler(1,
      (headerAddress: Long, headerSize: Long, amData: UcpAmData, _: UcpEndpoint) => {
      val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
      val workerId = UcxWorkerId.deserialize(header)
      val workerAddress = UnsafeUtils.getByteBufferView(amData.getDataAddress, amData.getLength.toInt)
      connectBack(workerId, workerAddress)
      UcsConstants.STATUS.UCS_OK
    }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG)
    globalWorker.setAmRecvHandler(2,
      (headerAddress: Long, headerSize: Long, amData: UcpAmData, _: UcpEndpoint) => {
      val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
      val workerId = UcxWorkerId.deserialize(header)
      val replyTag = header.getInt
      val exeId = header.getInt
      val blockId = UcxShuffleBlockId.deserialize(header)
      handleFetchBlockStream(workerId, exeId, replyTag, blockId)
      UcsConstants.STATUS.UCS_OK
    }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG )

    initProgressPool(serverConf.numListenerThreads + 1)

    logInfo(s"Allocating ${serverConf.numListenerThreads} server workers")

    allocatedWorker = new Array[ExternalUcxWorkerWrapper](serverConf.numListenerThreads)
    for (i <- 0 until serverConf.numListenerThreads) {
      val worker = ucxContext.newWorker(ucpWorkerParams)
      val workerId = new UcxWorkerId("Server", 0, i)
      allocatedWorker(i) = new ExternalUcxWorkerWrapper(worker, this, false, workerId)
      progressExecutors.execute(new ProgressTask(allocatedWorker(i).worker))
    }

    logInfo(s"Launching global workers")
    // Submit throws no exception except Future.get
    progressExecutors.execute(new ProgressTask(globalWorker))

    initialized = true
    logInfo(s"Started listener on ${listener.getAddress}")
    SerializationUtils.serializeInetAddress(listener.getAddress)
  }

  /**
   * Close all transport resources
   */
  override def close(): Unit = {
    if (initialized) {
      endpoints.foreach(_.closeNonBlockingForce())
      endpoints.clear()

      if (listener != null) {
        listener.close()
        listener = null
      }

      if (globalWorker != null) {
        globalWorker.close()
        globalWorker = null
      }

      replyExecutors.shutdown()

      super.close()
    }
  }

  def applicationRemoved(appId: String): Unit = {
    workerMap.remove(appId)
  }

  def executorRemoved(executorId: String, appId: String): Unit = {
    // val m = workerMap.get(appId)
    // if (m != null) {
    //   m.remove(executorId.toInt)
    // }
  }

  def connectBack(clientWorker: UcxWorkerId, workerAddress: ByteBuffer): Unit = {
    // TODO: need support application remove
    val copiedAddress = ByteBuffer.allocateDirect(workerAddress.remaining)
    copiedAddress.put(workerAddress)
    workerMap.getOrElseUpdate(clientWorker.appId, new TrieMap[Long, ByteBuffer])
      .getOrElseUpdate(UcxWorkerId.makeExeWorkerId(clientWorker), copiedAddress)
    replyExecutors.submit(new Runnable {
      override def run(): Unit = {
        allocatedWorker.foreach(_.getConnectionBack(clientWorker))
      }
    })
    allocatedWorker.foreach(_.connectBack(clientWorker, workerAddress))
  }

  def handleFetchBlockRequest(clientWorker: UcxWorkerId, exeId: Int, replyTag: Int, amData: UcpAmData): Unit = {
    replyExecutors.submit(new Runnable {
      override def run(): Unit = {
        val buffer = UnsafeUtils.getByteBufferView(amData.getDataAddress, amData.getLength.toInt)
        // val blockIds = mutable.ArrayBuffer.empty[UcxShuffleBlockId]
        val blockInfos = mutable.ArrayBuffer.empty[(Long, ReadableByteChannel)]

        // 1. Deserialize blockIds from header
        while (buffer.remaining() > 0) {
          val bid = UcxShuffleBlockId.deserialize(buffer)
          val buf = blockManager.getBlockData(clientWorker.appId,
            exeId.toString, bid.shuffleId, bid.mapId, bid.reduceId)
          val ch = Channels.newChannel(buf.createInputStream)
          blockInfos += buf.size() -> ch
        }

        amData.close()

        // val blocks = blockInfos.map{ case (length, ch) => {
        //   new Block {
        //     override def getBlock(byteBuffer: ByteBuffer): Unit = {
        //       ch.read(byteBuffer)
        //     }
        //     override def getSize: Long = length
        //   }
        // }}
        selectWorker.handleFetchBlockRequest(clientWorker, replyTag, blockInfos)
        blockInfos.foreach(_._2.close())
      }
    })
  }

  def handleFetchBlockStream(clientWorker: UcxWorkerId, exeId: Int,
                             replyTag: Int, bid: UcxShuffleBlockId): Unit = {
    replyExecutors.submit(new Runnable {
      override def run(): Unit = {
        val buf = blockManager.getBlockData(clientWorker.appId,
          exeId.toString, bid.shuffleId, bid.mapId, bid.reduceId)
        val ch = Channels.newChannel(buf.createInputStream)
        val blockInfo = buf.size() -> ch
        selectWorker.handleFetchBlockStream(clientWorker, replyTag, blockInfo)
      }
    })
  }

  def submit(task: Runnable): Unit = {
    replyExecutors.submit(task)
  }
}
