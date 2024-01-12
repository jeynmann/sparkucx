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

class ExternalUcxServerTransport(serverConf: ExternalUcxServerConf,
  val blockManager: ExternalUcxShuffleBlockResolver)
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
  private var handler: ExternalUcxServerWorker = _
  private var listener: UcpListener = _

  override def estimateNumEps(): Int = serverConf.ucxEpsNum

  override def init(): ByteBuffer = {
    initContext()
    initMemoryPool()
    initWorker()

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
    handler = new ExternalUcxServerWorker(worker, this, UcxWorkerId("", 0))

    logInfo(s"Allocating ${serverConf.numListenerThreads} threads")
    initProgressPool(serverConf.numListenerThreads, serverConf.useWakeup)

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

      if (handler != null) {
        handler.close()
        handler = null
      }

      super.close()
    }
  }

  def registerWorker(
    clientWorker: UcxWorkerId, workerAddress: ByteBuffer): Unit = {
      workerMap.getOrElseUpdate(
        clientWorker.appId, new TrieMap[Long, ByteBuffer])
        .getOrElseUpdate(UcxWorkerId.makeExeWorkerId(clientWorker), workerAddress)
    }

  def applicationRemoved(appId: String): Unit = {
    workerMap.remove(appId).map(clientAddress => {
      val shuffleClients = clientAddress.map(x => UcxWorkerId(appId, x._1))
      post(() => handler.disconnect(shuffleClients.toSeq))
    })
    handler.reportClients()
  }

  def executorRemoved(executorId: String, appId: String): Unit = {
    val exeId = executorId.toInt
    workerMap.get(appId).map(clientAddress => {
      val filteredAddress = clientAddress.filterKeys(
        id => UcxWorkerId.extractExeId(id) == exeId)
      val shuffleClients = filteredAddress.map(x => UcxWorkerId(appId, x._1))
      post(() => handler.disconnect(shuffleClients.toSeq))
    })
    // handler.reportClients()
  }
}
