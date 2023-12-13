package org.apache.spark.shuffle.ucx

// import org.apache.spark.SparkEnv
import org.apache.spark.network.util.TransportConf
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFileManager}
import org.apache.spark.shuffle.utils.{UcxLogging, UnsafeUtils}
import org.apache.spark.shuffle.ucx.memory.UcxHostBounceBuffersPool
import org.apache.spark.shuffle.ucx.utils.{SerializableDirectBuffer, SerializationUtils}
import org.apache.spark.storage.BlockManagerId
import org.openucx.jucx.ucp._

import java.net.InetSocketAddress
import java.nio.ByteBuffer

class UcxShuffleTransportClient(clientConf: ExternalUcxClientConf, blockManagerId: BlockManagerId)
extends ExternalShuffleTransport(clientConf) with UcxLogging {
  private[spark] val serverPort = clientConf.ucxServerPort
  private[spark] lazy val sparkTransportConf = SparkTransportConf.fromSparkConf(
    clientConf.getSparkConf, "shuffle", clientConf.numWorkers)

  private[this] class ProgressTask(worker: UcpWorker) extends Runnable {
    override def run(): Unit = {
      val useWakeup = ucxShuffleConf.useWakeup
      while (running) {
        worker.synchronized {
          while (worker.progress != 0) {}
        }
        if (useWakeup) {
          worker.waitForEvents()
        }
      }
    }
  }

  override def estimateNumEps(): Int = clientConf.numWorkers *
      clientConf.sparkConf.getInt("spark.executor.instances", 1)

  override def init(): ByteBuffer = {
    initContext()
    initMemoryPool()
  
    if (clientConf.useWakeup) {
      ucpWorkerParams.requestWakeupRX().requestWakeupTX().requestWakeupEdge()
    }

    initProgressPool(clientConf.numWorkers)

    logInfo(s"Allocating ${clientConf.numWorkers} client workers")
    val appId = clientConf.sparkConf.getAppId
    val exeId = blockManagerId.executorId.toLong
    allocatedWorker = new Array[ExternalUcxWorkerWrapper](clientConf.numWorkers)
    for (i <- 0 until clientConf.numWorkers) {
      ucpWorkerParams.setClientId((exeId << 32) | i.toLong)
      val worker = ucxContext.newWorker(ucpWorkerParams)
      val workerId = new UcxWorkerId(appId, exeId.toInt, i)
      allocatedWorker(i) = new ExternalUcxWorkerWrapper(worker, this, true, workerId)
      progressExecutors.execute(new ProgressTask(allocatedWorker(i).worker))
    }

    initialized = true
    val shuffleServer = new InetSocketAddress(blockManagerId.host, serverPort)
    logInfo(s"Shuffle server ${shuffleServer}")
    SerializationUtils.serializeInetAddress(shuffleServer)
  }

  override def initMemoryPool(): Unit = {
    hostBounceBufferMemoryPool = new UcxHostBounceBuffersPool(ucxContext)
    hostBounceBufferMemoryPool.init(clientConf.minRegistrationSize,
      clientConf.minBufferSize,
      clientConf.preallocateBuffersMap)
  }

  // override def selectWorker(): ExternalUcxWorkerWrapper = {
  //   allocatedWorker(
  //     (currentWorkerId.incrementAndGet() % allocatedWorker.length).abs)
  // }

  def connect(shuffleServer: SerializableDirectBuffer): Unit = {
    val addressBuffer = shuffleServer.value
    val address = SerializationUtils.deserializeInetAddress(addressBuffer)
    allocatedWorker.foreach(_.getConnection(address))
  }

  def connectAll(shuffleServerSet: Set[SerializableDirectBuffer]): Unit = {
    val addressSet = shuffleServerSet.map(addressBuffer =>
      SerializationUtils.deserializeInetAddress(addressBuffer.value))
    allocatedWorker.foreach(w => addressSet.foreach(w.getConnection(_)))
  }

  /**
   * Batch version of [[ fetchBlocksByBlockIds ]].
   */
  def fetchBlocksByBlockIds(host: String, exeId: Int,
                            blockIds: Seq[BlockId],
                            callbacks: Seq[OperationCallback]): Unit = {
    selectWorker.fetchBlocksByBlockIds(
      new InetSocketAddress(host, serverPort), exeId, blockIds, callbacks)
  }

  def fetchBlockByStream(host: String, exeId: Int, blockId: BlockId,
                         callback: OperationCallback): Unit = {
    selectWorker.fetchBlockByStream(
      new InetSocketAddress(host, serverPort), exeId, blockId, callback)
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
