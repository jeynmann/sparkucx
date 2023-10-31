package org.apache.spark.shuffle.ucx

// import org.apache.spark.SparkEnv
import org.apache.spark.shuffle.ucx.memory.UcxHostBounceBuffersPool
import org.apache.spark.shuffle.ucx.rpc.GlobalWorkerRpcThread
import org.apache.spark.shuffle.ucx.utils.{SerializableDirectBuffer, SerializationUtils}
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.network.shuffle.UcxLogging
import org.apache.spark.network.shuffle.ExternalUcxShuffleBlockResolver
import org.apache.spark.storage.BlockManagerId
import org.openucx.jucx.UcxException
import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class UcxShuffleTransportServer(
  serverConf: ExternalUcxServerConf, blockManager: ExternalUcxShuffleBlockResolver)
  extends ExternalShuffleTransport(serverConf)
  with UcxLogging {
  private val ucpWorkerParams = new UcpWorkerParams().requestThreadSafety()

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

  private var globalThread: ExternalUcxWorkerThread = _
  private var listener: UcpListener = _
  val endpoints = mutable.Set.empty[UcpEndpoint]

  private var allocatedServerThreads: Array[ExternalUcxWorkerThread] = _
  private val serverThreadId = new AtomicInteger()

  override def estimateNumEps(): Int = serverConf.ucxEpsNum

  override def init(): ByteBuffer = {
    super.initContext()
    super.initMemoryPool()

    if (serverConf.useWakeup) {
      ucpWorkerParams.requestWakeupRX().requestWakeupTX().requestWakeupEdge()
    }

    logInfo(s"Allocating global workers")
    val globalWorker = ucxContext.newWorker(ucpWorkerParams)
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
    globalThread = new ExternalUcxWorkerThread(globalWorker, this, false)

    allocatedServerThreads = new Array[ExternalUcxWorkerThread](serverConf.numListenerThreads)
    logInfo(s"Allocating ${serverConf.numListenerThreads} server workers")
    for (i <- 0 until serverConf.numListenerThreads) {
      val worker = ucxContext.newWorker(ucpWorkerParams)
      allocatedServerThreads(i) = new ExternalUcxWorkerThread(worker, this, false, new UcxWorkerId("Server", 0, i))
    }

    logInfo(s"Launch ${serverConf.numListenerThreads} server workers")
    allocatedServerThreads.foreach(_.start)
    globalThread.start

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

      if (globalThread != null) {
        globalThread.close()
        globalThread = null
      }

      allocatedServerThreads.foreach(_.close) 

      super.close()
    }
  }

  def connectBack(clientWorker: UcxWorkerId, workerAddress: ByteBuffer): Unit = {
    val copiedAddress = ByteBuffer.allocateDirect(workerAddress.remaining)
    copiedAddress.put(workerAddress)
    copiedAddress.rewind()
    allocatedServerThreads.foreach(t => t.submit(new Runnable {
      override def run(): Unit = {
        logInfo(s"ConnectBack ${t.workerId.workerId} to $clientWorker")
        t.workerWrapper.connectBack(clientWorker, copiedAddress)
      }
    }))
  }

  def handleFetchBlockRequest(clientWorker: UcxWorkerId, exeId: Int, replyTag: Int, amData: UcpAmData): Unit = {
    val server = selectServerThread
    server.submit(new Runnable {
      override def run(): Unit = {
        val buffer = UnsafeUtils.getByteBufferView(amData.getDataAddress, amData.getLength.toInt)
        val blockIds = mutable.ArrayBuffer.empty[UcxShuffleBockId]

        // 1. Deserialize blockIds from header
        while (buffer.remaining() > 0) {
          blockIds += UcxShuffleBockId.deserialize(buffer)
        }

        amData.close()

        val blocks = blockIds.map{ bid => {
          new Block {
            val blockBuffer = blockManager.getBlockData(
              clientWorker.appId.toString, exeId.toString, bid.shuffleId,
              bid.mapId, bid.reduceId)

            val blockChannel = Channels.newChannel(blockBuffer.createInputStream)

            override def getBlock(byteBuffer: ByteBuffer): Unit = {
              blockChannel.read(byteBuffer)
            }

            override def getSize: Long = blockBuffer.size()
          }
        }}
        server.workerWrapper.handleFetchBlockRequest(clientWorker, replyTag, blocks)
      }
    })
  }

  @inline
  def selectServerThread(): ExternalUcxWorkerThread = allocatedServerThreads(
    (serverThreadId.incrementAndGet() % allocatedServerThreads.length).abs)
}
