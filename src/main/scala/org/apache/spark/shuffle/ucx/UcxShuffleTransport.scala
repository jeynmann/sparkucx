/*
 * Copyright (C) 2022, NVIDIA CORPORATION & AFFILIATES. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.memory.UcxLimitedMemPool
import org.apache.spark.shuffle.ucx.rpc.GlobalWorkerRpcThread
import org.apache.spark.shuffle.ucx.utils.{SerializableDirectBuffer, SerializationUtils}
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.util.ThreadUtils
import org.apache.spark.network.netty.SparkTransportConf
import org.openucx.jucx.UcxException
import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants

import java.nio.ByteBuffer
import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean}
import scala.collection.concurrent.TrieMap

case class UcxShuffleBockId(shuffleId: Int, mapId: Int, reduceId: Int) extends BlockId {
  override def serializedSize: Int = UcxShuffleBockId.serializedSize

  override def serialize(byteBuffer: ByteBuffer): Unit = {
    byteBuffer.putInt(shuffleId)
    byteBuffer.putInt(mapId)
    byteBuffer.putInt(reduceId)
  }
}

object UcxShuffleBockId {
  def serializedSize: Int = 12
  def deserialize(byteBuffer: ByteBuffer): UcxShuffleBockId = {
    val shuffleId = byteBuffer.getInt
    val mapId = byteBuffer.getInt
    val reduceId = byteBuffer.getInt
    UcxShuffleBockId(shuffleId, mapId, reduceId)
  }
}

object UcxAmId {
  // client -> server
  final val FETCH_BLOCK = 1
  final val FETCH_STREAM = 2
  // server -> client
  final val REPLY_BLOCK = 1
  final val REPLY_STREAM = 2
  final val REPLY_SLICE = 3
}

class UcxShuffleTransport(var ucxShuffleConf: UcxShuffleConf = null, var executorId: Long = 0) extends ShuffleTransport
  with Logging {
  @volatile private var initialized: Boolean = false
  private[ucx] var ucxContext: UcpContext = _
  private var globalWorker: UcpWorker = _
  private val ucpWorkerParams = new UcpWorkerParams().requestThreadSafety()
  val executorAddresses = new TrieMap[ExecutorId, ByteBuffer]

  private val running = new AtomicBoolean(true)
  private val progressThread = ThreadUtils.newDaemonFixedThreadPool(
     ucxShuffleConf.numWorkers, "UCX-client")
  private var allocatedClientWorkers: Array[UcxWorkerWrapper] = _
  private var clientWorkerId = new AtomicInteger()
  private var clientLocal = new ThreadLocal[UcxWorkerWrapper]

  private[ucx] val registeredBlocks = new TrieMap[BlockId, Block]
  private[ucx] var globalThread: GlobalWorkerRpcThread = _

  var hostBounceBufferMemoryPool: UcxLimitedMemPool = _
  var serverBounceBufferMemoryPool: UcxLimitedMemPool = _

  private[spark] val timeout = ucxShuffleConf.getSparkConf.getTimeAsMs(
    "spark.network.timeout", "10s")
  private[spark] lazy val sparkTransportConf = SparkTransportConf.fromSparkConf(
    ucxShuffleConf, "shuffle", ucxShuffleConf.numWorkers)
  private[spark] lazy val maxBlocksPerRequest = maxBlocksInAmHeader.min(
    ucxShuffleConf.maxBlocksPerRequest).toInt

  private[ucx] class ProgressTask(wrapper: UcxWorkerWrapper) extends Runnable {
    override def run(): Unit = {
      clientLocal.set(wrapper)
      try {
        val worker = wrapper.worker
        while (running.get()) {
          worker.synchronized {
            while (worker.progress != 0) {}
          }
          worker.waitForEvents()
        }
      } finally {
        clientLocal.set(null)
      }
    }
  }

  override def init(): ByteBuffer = {
    if (ucxShuffleConf == null) {
      ucxShuffleConf = new UcxShuffleConf(SparkEnv.get.conf)
    }

    val numEndpoints = ucxShuffleConf.numWorkers *
      ucxShuffleConf.getSparkConf.getInt("spark.executor.instances", 1) *
      ucxShuffleConf.numListenerThreads // Each listener thread creates backward endpoint
    logInfo(s"Creating UCX context with an estimated number of endpoints: $numEndpoints")

    val params = new UcpParams().requestAmFeature().setMtWorkersShared(true).setEstimatedNumEps(numEndpoints)
      .requestAmFeature().setConfig("USE_MT_MUTEX", "yes")

    if (ucxShuffleConf.useWakeup) {
      params.requestWakeupFeature()
      ucpWorkerParams.requestWakeupRX().requestWakeupTX().requestWakeupEdge()
    }

    ucxContext = new UcpContext(params)
    val maxSizeInFlight = ucxShuffleConf.getSparkConf.getSizeAsBytes("spark.reducer.maxSizeInFlight", "48m")
    val serverMaxRegSize = (ucxShuffleConf.maxRegistrationSize / 2L).min(
      maxSizeInFlight.max(ucxShuffleConf.maxReplySize * 4L) *
      ucxShuffleConf.numListenerThreads.toLong)
    val clientMaxRegSize = ucxShuffleConf.maxRegistrationSize - serverMaxRegSize
    hostBounceBufferMemoryPool = new UcxLimitedMemPool(ucxContext)
    hostBounceBufferMemoryPool.init(
      ucxShuffleConf.minBufferSize, ucxShuffleConf.maxBufferSize,
      ucxShuffleConf.minRegistrationSize, clientMaxRegSize,
      ucxShuffleConf.preallocateBuffersMap, ucxShuffleConf.memoryLimit)
    serverBounceBufferMemoryPool = new UcxLimitedMemPool(ucxContext)
    serverBounceBufferMemoryPool.init(
      ucxShuffleConf.minBufferSize, ucxShuffleConf.maxBufferSize,
      ucxShuffleConf.minRegistrationSize, serverMaxRegSize,
      Map[Long, Int](), ucxShuffleConf.memoryLimit)

    globalWorker = ucxContext.newWorker(ucpWorkerParams)

    globalThread = new GlobalWorkerRpcThread(globalWorker, this)
    globalThread.start()

    allocatedClientWorkers = new Array[UcxWorkerWrapper](ucxShuffleConf.numWorkers)
    logInfo(s"Allocating ${ucxShuffleConf.numWorkers} client workers")
    for (i <- 0 until ucxShuffleConf.numWorkers) {
      val clientId: Long = ((i.toLong + 1L) << 32) | executorId
      ucpWorkerParams.setClientId(clientId)
      val worker = ucxContext.newWorker(ucpWorkerParams)
      val wrapper = UcxWorkerWrapper(worker, this, isClientWorker = true, clientId)
      allocatedClientWorkers(i) = wrapper
      progressThread.submit(new ProgressTask(wrapper))
    }

    initialized = true
    logInfo(s"Started listener on ${globalThread.listener.getAddress}")
    SerializationUtils.serializeInetAddress(globalThread.listener.getAddress)
    // globalWorker.getAddress()
  }

  /**
   * Close all transport resources
   */
  override def close(): Unit = {
    if (initialized) {
      running.set(false)

      hostBounceBufferMemoryPool.close()
      serverBounceBufferMemoryPool.close()

      allocatedClientWorkers.foreach(_.close())

      if (globalThread != null) {
        globalThread.interrupt()
        globalThread.join(10)
      }

      if (ucxContext != null) {
        ucxContext.close()
        ucxContext = null
      }
    }
  }

  def maxBlocksInAmHeader(): Long = {
    (globalWorker.getMaxAmHeaderSize - 2) / UnsafeUtils.INT_SIZE
  }

  /**
   * Add executor's worker address. For standalone testing purpose and for implementations that makes
   * connection establishment outside of UcxShuffleManager.
   */
  override def addExecutor(executorId: ExecutorId, workerAddress: ByteBuffer): Unit = {
    executorAddresses.put(executorId, workerAddress)
    allocatedClientWorkers.foreach(_.getConnection(executorId))
  }

  def addExecutors(executorIdsToAddress: Map[ExecutorId, SerializableDirectBuffer]): Unit = {
    executorIdsToAddress.foreach {
      case (executorId, address) => executorAddresses.put(executorId, address.value)
    }
  }

  def preConnect(): Unit = {
    allocatedClientWorkers.foreach(_.preconnect())
  }

  /**
   * Remove executor from communications.
   */
  override def removeExecutor(executorId: Long): Unit = executorAddresses.remove(executorId)

  /**
   * Registers blocks using blockId on SERVER side.
   */
  override def register(blockId: BlockId, block: Block): Unit = {
    registeredBlocks.put(blockId, block)
  }

  /**
   * Change location of underlying blockId in memory
   */
  override def mutate(blockId: BlockId, newBlock: Block, callback: OperationCallback): Unit = {
    unregister(blockId)
    register(blockId, newBlock)
    callback.onComplete(new OperationResult {
      override def getStatus: OperationStatus.Value = OperationStatus.SUCCESS

      override def getError: TransportError = null

      override def getStats: Option[OperationStats] = None

      override def getData: MemoryBlock = newBlock.getMemoryBlock
    })

  }

  /**
   * Indicate that this blockId is not needed any more by an application.
   * Note: this is a blocking call. On return it's safe to free blocks memory.
   */
  override def unregister(blockId: BlockId): Unit = {
    registeredBlocks.remove(blockId)
  }

  def unregisterShuffle(shuffleId: Int): Unit = {
    registeredBlocks.keysIterator.foreach(bid =>
      if (bid.asInstanceOf[UcxShuffleBockId].shuffleId == shuffleId) {
        registeredBlocks.remove(bid)
      }
    )
  }

  def unregisterAllBlocks(): Unit = {
    registeredBlocks.clear()
  }

  /**
   * Batch version of [[ fetchBlocksByBlockIds ]].
   */
  override def fetchBlocksByBlockIds(executorId: ExecutorId, blockIds: Seq[BlockId],
                                     resultBufferAllocator: BufferAllocator,
                                     callbacks: Seq[OperationCallback]): Unit = {
    selectClientWorker.fetchBlocksByBlockIds(executorId, blockIds,
                                             resultBufferAllocator, callbacks)
  }

  def fetchBlockByStream(executorId: ExecutorId, blockId: BlockId,
                         resultBufferAllocator: BufferAllocator,
                         callback: OperationCallback): Unit = {
    selectClientWorker.fetchBlockByStream(executorId, blockId,
                                          resultBufferAllocator, callback)
  }

  @inline
  def selectClientWorker(): UcxWorkerWrapper = Option(clientLocal.get) match {
    case Some(worker) => worker
    case None =>
      val worker = allocatedClientWorkers(
        (clientWorkerId.incrementAndGet() % allocatedClientWorkers.length).abs)
      clientLocal.set(worker)
      worker
  }

  /**
   * Progress outstanding operations. This routine is blocking (though may poll for event).
   * It's required to call this routine within same thread that submitted [[ fetchBlocksByBlockIds ]].
   *
   * Return from this method guarantees that at least some operation was progressed.
   * But not guaranteed that at least one [[ fetchBlocksByBlockIds ]] completed!
   */
  override def progress(): Unit = {
  }
}
