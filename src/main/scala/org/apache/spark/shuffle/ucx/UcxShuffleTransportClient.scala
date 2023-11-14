package org.apache.spark.shuffle.ucx

// import org.apache.spark.SparkEnv
import org.apache.spark.shuffle.utils.UcxLogging
import org.apache.spark.shuffle.ucx.memory.UcxHostBounceBuffersPool
import org.apache.spark.shuffle.ucx.utils.{SerializableDirectBuffer, SerializationUtils}
import org.apache.spark.storage.BlockManagerId
import org.openucx.jucx.ucp._

import java.net.InetSocketAddress
import java.nio.ByteBuffer

class UcxShuffleTransportClient(clientConf: ExternalUcxClientConf, blockManagerId: BlockManagerId)
extends ExternalShuffleTransport(clientConf) with UcxLogging {
  private[spark] val serverPort = clientConf.ucxServerPort

  class ProgressTask(worker: UcpWorker) extends Runnable {
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

  @inline
  def selectWorker(): ExternalUcxWorkerWrapper = {
    allocatedWorker(
      (currentWorkerId.incrementAndGet() % allocatedWorker.length).abs)
  }
}
