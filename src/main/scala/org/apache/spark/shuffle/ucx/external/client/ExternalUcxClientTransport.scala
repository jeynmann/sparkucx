package org.apache.spark.shuffle.ucx

import scala.collection.concurrent.TrieMap
// import org.apache.spark.SparkEnv
import org.apache.spark.network.util.TransportConf
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFileManager}
import org.apache.spark.shuffle.utils.{UcxLogging, UnsafeUtils}
import org.apache.spark.shuffle.ucx.memory.UcxLimitedMemPool
import org.apache.spark.shuffle.ucx.utils.{SerializableDirectBuffer, SerializationUtils}
import org.apache.spark.storage.BlockManagerId
import org.openucx.jucx.ucp._

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

class ExternalUcxClientTransport(val clientConf: ExternalUcxClientConf, val blockManagerId: BlockManagerId)
  extends ExternalUcxTransport(clientConf) with UcxLogging {
  private[spark] val timeout = clientConf.getSparkConf.getTimeAsMs(
    "spark.network.timeout", "10s")
  private[spark] lazy val sparkTransportConf = SparkTransportConf.fromSparkConf(
    clientConf.getSparkConf, "ucx-shuffle", clientConf.numWorkers)
  private[spark] lazy val maxBlocksPerRequest = maxBlocksInAmHeader.min(
    clientConf.maxBlocksPerRequest).toInt

  private[ucx] val tcpServer = new InetSocketAddress(blockManagerId.host,
                                                     blockManagerId.port)
  private[ucx] val ucxServer = new InetSocketAddress(tcpServer.getHostName,
                                                     clientConf.ucxServerPort)
  private[ucx] val serverMap = new TrieMap[InetSocketAddress, ByteBuffer]

  private var handler: ExternalUcxClientWorker = _

  override def estimateNumEps(): Int = clientConf.numWorkers *
      clientConf.sparkConf.getInt("spark.executor.instances", 1)

  override def init(): ByteBuffer = {
    initContext()
    initMemoryPool()

    val appId = clientConf.sparkConf.getAppId
    val exeId = blockManagerId.executorId.toLong
    ucpWorkerParams.setClientId(exeId)
    initWorker()

    val workerId = new UcxWorkerId(appId, exeId.toInt, 0)
    handler = new ExternalUcxClientWorker(worker, this, workerId)

    logInfo(s"Allocating ${clientConf.numWorkers} client threads")
    initProgressPool(clientConf.numWorkers, clientConf.useWakeup)

    initialized = true
    logInfo(s"Transport init done. ${blockManagerId}")
    SerializationUtils.serializeInetAddress(ucxServer)
  }

  override def close(): Unit = {
    if (initialized) {
      if (handler != null) {
        handler.close()
        handler = null
      }
      super.close()
    }
  }

  def maxBlocksInAmHeader(): Long = {
    (worker.getMaxAmHeaderSize - 2) / UnsafeUtils.INT_SIZE
  }

  def connect(serverBuffer: SerializableDirectBuffer,
              addressBuffer: SerializableDirectBuffer): Unit = {
    val server = SerializationUtils.deserializeInetAddress(serverBuffer.value)
    serverMap.getOrElseUpdate(server, addressBuffer.value)
    handler.getConnection(server)
  }

  def connectAll(serverAddressMap: Map[SerializableDirectBuffer,
                                       SerializableDirectBuffer]): Unit = {
    val serverSet = serverAddressMap.map(serverAddress => {
      val server = SerializationUtils.deserializeInetAddress(
        serverAddress._1.value)
      val ucpAddress = serverAddress._2.value
      serverMap.getOrElseUpdate(server, ucpAddress)
      server
    })
    serverSet.foreach(handler.getConnection(_))
  }

  /**
   * Batch version of [[ fetchBlocksByBlockIds ]].
   */
  def fetchBlocksByBlockIds(host: String, port: Int, exeId: Int,
                            blockIds: Seq[BlockId],
                            callbacks: Seq[OperationCallback]): Unit = {
    val server = new InetSocketAddress(host, port)
    handler.fetchBlocksByBlockIds(server, exeId, blockIds, callbacks)
  }

  def fetchBlockByStream(host: String, port: Int, exeId: Int,
                         blockId: BlockId,
                         callback: OperationCallback): Unit = {
    val server = new InetSocketAddress(host, port)
    handler.fetchBlockByStream(server, exeId, blockId, callback)
  }
}

private[shuffle] class UcxFetchCallBack(
  blockId: String, listener: BlockFetchingListener)
  extends OperationCallback {

  override def onComplete(result: OperationResult): Unit = {
    val memBlock = result.getData
    val buffer = UnsafeUtils.getByteBufferView(memBlock.address,
                                               memBlock.size.toInt)
    listener.onBlockFetchSuccess(blockId, new NioManagedBuffer(buffer) {
      override def release: ManagedBuffer = {
        memBlock.close()
        this
      }
    })
  }
}

private[shuffle] class UcxDownloadCallBack(
  blockId: String, listener: BlockFetchingListener,
  downloadFileManager: DownloadFileManager,
  transportConf: TransportConf)
  extends OperationCallback {

  private[this] val targetFile = downloadFileManager.createTempFile(
    transportConf)
  private[this] val channel = targetFile.openForWriting()

  override def onData(buffer: ByteBuffer): Unit = {
    while (buffer.hasRemaining()) {
      channel.write(buffer)
    }
  }

  override def onComplete(result: OperationResult): Unit = {
    listener.onBlockFetchSuccess(blockId, channel.closeAndRead())
    if (!downloadFileManager.registerTempFileToClean(targetFile)) {
      targetFile.delete()
    }
  }
}
