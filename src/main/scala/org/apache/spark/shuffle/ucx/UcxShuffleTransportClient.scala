package org.apache.spark.shuffle.ucx

// import org.apache.spark.SparkEnv
import org.apache.spark.shuffle.utils.UcxLogging
import org.apache.spark.shuffle.ucx.memory.UcxHostBounceBuffersPool
import org.apache.spark.shuffle.ucx.utils.{SerializableDirectBuffer, SerializationUtils}
import org.apache.spark.storage.BlockManagerId
import org.openucx.jucx.ucp._

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

class UcxShuffleTransportClient(clientConf: ExternalUcxClientConf, blockManagerId: BlockManagerId)
  extends ExternalShuffleTransport(clientConf) with UcxLogging {
  private val ucpWorkerParams = new UcpWorkerParams().requestThreadSafety()

  private var allocatedClientThreads: Array[ExternalUcxWorkerThread] = _
  private var clientThreadId = new AtomicInteger()

  val serverPort = clientConf.ucxServerPort

  override def estimateNumEps(): Int = clientConf.numWorkers *
      clientConf.sparkConf.getInt("spark.executor.instances", 1)

  override def init(): ByteBuffer = {
    super.initContext()
    super.initMemoryPool()
  
    if (clientConf.useWakeup) {
      ucpWorkerParams.requestWakeupRX().requestWakeupTX().requestWakeupEdge()
    }

    allocatedClientThreads = new Array[ExternalUcxWorkerThread](clientConf.numWorkers)
    logInfo(s"Allocating ${clientConf.numWorkers} client workers")
    val appId = clientConf.sparkConf.getAppId
    val exeId = blockManagerId.executorId.toLong.toInt
    for (i <- 0 until clientConf.numWorkers) {
      val workerId = new UcxWorkerId(appId, exeId, i)
      ucpWorkerParams.setClientId((workerId.exeId.toLong << 32) | workerId.workerId.toLong)
      val worker = ucxContext.newWorker(ucpWorkerParams)
      allocatedClientThreads(i) = new ExternalUcxWorkerThread(worker, this, isClientWorker = true, workerId)
    }

    logInfo(s"Launch ${clientConf.numWorkers} client workers")
    allocatedClientThreads.foreach(_.start)
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
    allocatedClientThreads.foreach { t => t.submit(new Runnable {
      override def run(): Unit = {
        logInfo(s"Connect ${t.workerId.workerId} to $address")
        t.workerWrapper.connect(address, addressBuffer)
        t.workerWrapper.progressConnect()
      }})
    }
  }

  def connectAll(shuffleServerSet: Set[SerializableDirectBuffer]): Unit = {
    val addressSet = shuffleServerSet.map {
      x => (SerializationUtils.deserializeInetAddress(x.value), x.value)
    }
    allocatedClientThreads.foreach { t => t.submit(new Runnable {
      override def run(): Unit = {
        addressSet.foreach { case (address, addressBuffer) => {
          logInfo(s"ConnectAll ${t.workerId.workerId} to $address")
          t.workerWrapper.connect(address, addressBuffer)
        }}
        t.workerWrapper.progressConnect()
      }
    })}
  }

  /**
   * Close all transport resources
   */
  override def close(): Unit = {
    if (initialized) {
      allocatedClientThreads.foreach(_.close)
      super.close()
    }
  }

  /**
   * Batch version of [[ fetchBlocksByBlockIds ]].
   */
  def fetchBlocksByBlockIds(shuffleServer: InetSocketAddress, exeId: Int,
                            blockIds: Seq[BlockId],
                            callbacks: Seq[OperationCallback]): Unit = {
    val client = selectClientThread
    client.submit(new Runnable {
      override def run(): Unit = {
        client.workerWrapper.fetchBlocksByBlockIds(shuffleServer, exeId, blockIds, callbacks)
      }
    })
  }

  @inline
  def selectClientThread(): ExternalUcxWorkerThread = allocatedClientThreads(
    (clientThreadId.incrementAndGet() % allocatedClientThreads.length).abs)
}
