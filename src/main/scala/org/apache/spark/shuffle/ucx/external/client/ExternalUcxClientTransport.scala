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

class ExternalUcxClientTransport(clientConf: ExternalUcxClientConf, blockManagerId: BlockManagerId)
extends ExternalUcxTransport(clientConf) with UcxLogging {
  private[spark] val tcpServerPort = blockManagerId.port
  private[spark] val ucxServerPort = clientConf.ucxServerPort
  private[spark] lazy val sparkTransportConf = SparkTransportConf.fromSparkConf(
    clientConf.getSparkConf, "ucx-shuffle", clientConf.numWorkers)

  private[ucx] lazy val currentWorkerId = new AtomicInteger()
  private[ucx] lazy val workerLocal = new ThreadLocal[ExternalUcxClientWorker]
  private[ucx] var allocatedWorker: Array[ExternalUcxClientWorker] = _

  private var maxBlocksPerRequest = 0

  override def estimateNumEps(): Int = clientConf.numWorkers *
      clientConf.sparkConf.getInt("spark.executor.instances", 1)

  override def init(): ByteBuffer = {
    initContext()
    initMemoryPool()
  
    if (clientConf.useWakeup) {
      ucpWorkerParams.requestWakeupRX().requestWakeupTX().requestWakeupEdge()
    }

    initTaskPool(clientConf.numThreads)

    logInfo(s"Allocating ${clientConf.numWorkers} client workers")
    val appId = clientConf.sparkConf.getAppId
    val exeId = blockManagerId.executorId.toLong
    allocatedWorker = new Array[ExternalUcxClientWorker](clientConf.numWorkers)
    for (i <- 0 until clientConf.numWorkers) {
      ucpWorkerParams.setClientId((exeId << 32) | i.toLong)
      val worker = ucxContext.newWorker(ucpWorkerParams)
      val workerId = new UcxWorkerId(appId, exeId.toInt, i)
      allocatedWorker(i) = new ExternalUcxClientWorker(worker, this, workerId)
    }

    logInfo(s"Launching ${clientConf.numWorkers} client workers")
    allocatedWorker.foreach(_.start)

    maxBlocksPerRequest = clientConf.maxBlocksPerRequest.min(
      (allocatedWorker(0).worker.getMaxAmHeaderSize - 2).toInt /
      UnsafeUtils.INT_SIZE)

    initialized = true
    val shuffleServer = new InetSocketAddress(blockManagerId.host, ucxServerPort)
    logInfo(s"Shuffle server ${shuffleServer}")
    allocatedWorker.foreach(_.connect(shuffleServer))
    SerializationUtils.serializeInetAddress(shuffleServer)
  }

  override def close(): Unit = {
    if (initialized) {
      running = false

      if (allocatedWorker != null) {
        allocatedWorker.foreach(_.close)
      }

      super.close()
    }
  }

  def getMaxBlocksPerRequest: Int = maxBlocksPerRequest
  // @inline
  // def selectWorker(): ExternalUcxClientWorker = {
  //   allocatedWorker(
  //     (currentWorkerId.incrementAndGet() % allocatedWorker.length).abs)
  // }

  @inline
  def selectWorker(): ExternalUcxClientWorker = {
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

  def connect(shuffleServer: SerializableDirectBuffer): Unit = {
    val addressBuffer = shuffleServer.value
    val address = SerializationUtils.deserializeInetAddress(addressBuffer)
    logDebug(s"connect $address")
    allocatedWorker.foreach(_.connect(address))
  }

  def connectAll(shuffleServerSet: Set[SerializableDirectBuffer]): Unit = {
    val addressSet = shuffleServerSet.map(addressBuffer =>
      SerializationUtils.deserializeInetAddress(addressBuffer.value))
    logDebug(s"connectAll $addressSet")
    allocatedWorker.foreach(_.connectAll(addressSet))
  }

  /**
   * Batch version of [[ fetchBlocksByBlockIds ]].
   */
  def fetchBlocksByBlockIds(host: String, exeId: Int,
                            blockIds: Seq[BlockId],
                            callbacks: Seq[OperationCallback]): Unit = {
    selectWorker.fetchBlocksByBlockIds(host, exeId, blockIds, callbacks)
  }

  def fetchBlockByStream(host: String, exeId: Int, blockId: BlockId,
                         callback: OperationCallback): Unit = {
    selectWorker.fetchBlockByStream(host, exeId, blockId, callback)
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
  private[this] val channel = targetFile.openForWriting();

  override def onData(buffer: ByteBuffer): Unit = {
    while (buffer.hasRemaining()) {
      channel.write(buffer);
    }
  }

  override def onComplete(result: OperationResult): Unit = {
    listener.onBlockFetchSuccess(blockId, channel.closeAndRead());
    if (!downloadFileManager.registerTempFileToClean(targetFile)) {
      targetFile.delete();
    }
  }
}
