package org.apache.spark.shuffle.ucx

// import org.apache.spark.SparkEnv
import org.apache.spark.shuffle.utils.{UcxLogging, UnsafeUtils}
import org.apache.spark.shuffle.ucx.utils.SerializationUtils
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.ExternalUcxShuffleBlockResolver
import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants
import org.openucx.jucx.ucs.UcsConstants.MEMORY_TYPE
import org.openucx.jucx.{UcxCallback, UcxException, UcxUtils}

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class ExternalUcxServerTransport(
  serverConf: ExternalUcxServerConf, blockManager: ExternalUcxShuffleBlockResolver)
  extends ExternalUcxTransport(serverConf)
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

  private val endpoints = mutable.Set.empty[UcpEndpoint]
  private var listener: UcpListener = _

  private[ucx] lazy val currentWorkerId = new AtomicInteger()
  private[ucx] lazy val workerLocal = new ThreadLocal[ExternalUcxServerWorker]
  private[ucx] var allocatedWorker: Array[ExternalUcxServerWorker] = _

  override def estimateNumEps(): Int = serverConf.ucxEpsNum

  override def init(): ByteBuffer = {
    initContext()
    initMemoryPool()

    if (serverConf.useWakeup) {
      ucpWorkerParams.requestWakeupRX().requestWakeupTX().requestWakeupEdge()
    }

    initTaskPool(serverConf.numWorkers + 1)

    logInfo(s"Allocating ${serverConf.numWorkers} server workers")

    allocatedWorker = new Array[ExternalUcxServerWorker](serverConf.numWorkers)
    for (i <- 0 until serverConf.numWorkers) {
      val worker = ucxContext.newWorker(ucpWorkerParams)
      val workerId = new UcxWorkerId("Server", 0, i)
      if (i == 0) {
        listener = worker.newListener(new UcpListenerParams().setSockAddr(
          new InetSocketAddress("0.0.0.0", serverConf.ucxServerPort))
          .setConnectionHandler((ucpConnectionRequest: UcpConnectionRequest) => {
            val id = ucpConnectionRequest.getClientId()
            val ep = worker.newEndpoint(
              new UcpEndpointParams().setConnectionRequest(ucpConnectionRequest)
                .setPeerErrorHandlingMode().setErrorHandler(errorHandler)
                .setName(s"Endpoint to $id"))
            endpoints.add(ep)
          }))
      }
      allocatedWorker(i) = new ExternalUcxServerWorker(worker, this, workerId)
    }

    logInfo(s"Launching ${serverConf.numWorkers} server workers")
    allocatedWorker.foreach(_.start)

    initialized = true
    logInfo(s"Started listener on ${listener.getAddress}")
    SerializationUtils.serializeInetAddress(listener.getAddress)
  }

  /**
   * Close all transport resources
   */
  override def close(): Unit = {
    if (initialized) {
      running = false

      allocatedWorker(0).close(() => {
        if (listener != null) {          
          endpoints.foreach(_.closeNonBlockingForce())
          endpoints.clear()

          listener.close()
          listener = null
        }
      })

      if (allocatedWorker != null) {
        allocatedWorker.foreach(_.close)
      }

      super.close()

      logInfo("UCX transport closed.")
    }
  }

  def applicationRemoved(appId: String): Unit = {
    workerMap.remove(appId).map(clientAddress => {
      val shuffleClients = clientAddress.map(x => UcxWorkerId(appId, x._1))
      allocatedWorker.foreach(_.disconnect(shuffleClients.toSeq))
    })
    // allocatedWorker.foreach(_.debugClients())
  }

  def executorRemoved(executorId: String, appId: String): Unit = {
    val exeId = executorId.toInt
    workerMap.get(appId).map(clientAddress => {
      val filteredAddress = clientAddress.filterKeys(
        id => UcxWorkerId.extractExeId(id) == exeId)
      val shuffleClients = filteredAddress.map(x => UcxWorkerId(appId, x._1))
      allocatedWorker.foreach(_.disconnect(shuffleClients.toSeq))
    })
  }

  def handleConnect(clientWorker: UcxWorkerId, address: ByteBuffer): Unit = {
    submit(new Runnable {
      override def run(): Unit = {
        allocatedWorker.foreach(_.connectBack(clientWorker, address))
        workerMap.getOrElseUpdate(clientWorker.appId, new TrieMap[Long, ByteBuffer])
          .getOrElseUpdate(UcxWorkerId.makeExeWorkerId(clientWorker), address)
      }
    })
  }

  def handleFetchBlockRequest(clientWorker: UcxWorkerId, exeId: Int,
                              replyTag: Int, blockIds: Seq[UcxShuffleBlockId]):
                              Unit = {
    submit(new Runnable {
      override def run(): Unit = {
        val blockInfos = blockIds.map(bid => {
          val buf = blockManager.getBlockData(clientWorker.appId, exeId.toString,
                                              bid.shuffleId, bid.mapId,
                                              bid.reduceId)
          buf.size() -> buf
        })
        selectWorker.handleFetchBlockRequest(clientWorker, replyTag, blockInfos)
      }
    })
  }

  def handleFetchBlockStream(clientWorker: UcxWorkerId, exeId: Int,
                             replyTag: Int, bid: UcxShuffleBlockId): Unit = {
    submit(new Runnable {
      override def run(): Unit = {
        val buf = blockManager.getBlockData(clientWorker.appId, exeId.toString,
                                            bid.shuffleId, bid.mapId,
                                            bid.reduceId)
        val blockInfo = buf.size() -> buf
        selectWorker.handleFetchBlockStream(clientWorker, replyTag, blockInfo)
      }
    })
  }

  @inline
  def selectWorker(): ExternalUcxServerWorker = {
    Option(workerLocal.get) match {
      case Some(worker) => worker
      case None => {
        val worker = allocatedWorker(
          (currentWorkerId.incrementAndGet() % allocatedWorker.length).abs)
        workerLocal.set(worker)
        worker
      }
    }
  }
}
