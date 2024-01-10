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
import org.apache.spark.util.Utils
import org.apache.spark.util.ThreadUtils
import org.apache.spark.network.netty.SparkTransportConf
import org.openucx.jucx.UcxException
import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

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

private[ucx] class UcxWorkerThread(worker: UcpWorker, useWakeup: Boolean) extends Thread {
  private val taskQueue = new ConcurrentLinkedQueue[Runnable]
  private val waiting = new AtomicInteger(0)

  setDaemon(true)
  setName("UCX-worker")

  def submit(task: Runnable): Unit = {
    taskQueue.add(task)
    if (waiting.compareAndSet(1, 0)) {
      worker.signal()
    }
  }

  override def run(): Unit = {
    while (!isInterrupted) {
      val task = taskQueue.poll()
      if (task != null) {
        task.run()
      }
      worker.synchronized {
        while (worker.progress != 0) {}
      }
      if (useWakeup && taskQueue.isEmpty && waiting.compareAndSet(0, 1)) {
        worker.waitForEvents()
      }
    }
  }
}

class UcxShuffleTransport(var ucxShuffleConf: UcxShuffleConf = null, var executorId: Long = 0) extends ShuffleTransport
  with Logging {
  @volatile private var initialized: Boolean = false
  private[ucx] var ucxContext: UcpContext = _
  private var globalWorker: UcpWorker = _
  private var globalWapper: UcxWorkerWrapper = _
  private var listener: UcpListener = _
  private val ucpWorkerParams = new UcpWorkerParams().requestThreadSafety()
  val endpoints = mutable.Set.empty[UcpEndpoint]
  val executorAddresses = new TrieMap[ExecutorId, ByteBuffer]

  private[ucx] val registeredBlocks = new TrieMap[BlockId, Block]
  private[ucx] var progressThread: UcxWorkerThread = _

  var hostBounceBufferMemoryPool: UcxLimitedMemPool = _

  private[spark] lazy val replyThreadPool = ThreadUtils.newForkJoinPool(
    "UcxListenerThread", ucxShuffleConf.numListenerThreads)
  private[spark] lazy val sparkTransportConf = SparkTransportConf.fromSparkConf(
    ucxShuffleConf, "shuffle", ucxShuffleConf.numWorkers)
  private[spark] lazy val maxBlocksPerRequest = maxBlocksInAmHeader.min(
    ucxShuffleConf.maxBlocksPerRequest).toInt

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

    hostBounceBufferMemoryPool = new UcxLimitedMemPool(ucxContext)
    hostBounceBufferMemoryPool.init(
      ucxShuffleConf.minBufferSize, ucxShuffleConf.maxBufferSize,
      ucxShuffleConf.minRegistrationSize, ucxShuffleConf.maxRegistrationSize,
      ucxShuffleConf.preallocateBuffersMap, ucxShuffleConf.memoryLimit)

    globalWorker = ucxContext.newWorker(ucpWorkerParams)
    globalWapper = UcxWorkerWrapper(globalWorker, this, executorId)

    val Array(host, port) = ucxShuffleConf.listenerAddress.split(":")
    listener = globalWorker.newListener(new UcpListenerParams().setSockAddr(
      new InetSocketAddress(Utils.localCanonicalHostName, port.toInt))
      .setConnectionHandler((ucpConnectionRequest: UcpConnectionRequest) => {
        val executorId = ucpConnectionRequest.getClientId
        val ep = globalWorker.newEndpoint(
          new UcpEndpointParams().setConnectionRequest(ucpConnectionRequest)
                                 .setPeerErrorHandlingMode()
                                 .setErrorHandler(errorHandler)
                                 .setName(s"Endpoint to $executorId"))
        globalWapper.connections.getOrElseUpdate(executorId, ep)
      }))

    progressThread = new UcxWorkerThread(globalWorker, ucxShuffleConf.useWakeup)
    progressThread.start()

    initialized = true
    logInfo(s"Started listener on ${listener.getAddress}")
    globalWorker.getAddress()
  }

  /**
   * Close all transport resources
   */
  override def close(): Unit = {
    if (initialized) {
      endpoints.foreach(_.closeNonBlockingForce())
      endpoints.clear()

      hostBounceBufferMemoryPool.close()

      if (listener != null) {
        listener.close()
        listener = null
      }

      if (progressThread != null) {
        progressThread.interrupt()
        progressThread.join(10)
      }

      if (globalWorker != null) {
        globalWorker.close()
        globalWorker = null
      }

      if (ucxContext != null) {
        ucxContext.close()
        ucxContext = null
      }
    }
  }

  @inline
  def maxBlocksInAmHeader(): Long = {
    (globalWorker.getMaxAmHeaderSize - 2) / UnsafeUtils.INT_SIZE
  }

  @inline
  def submit(task: Runnable): Unit = {
    replyThreadPool.submit(task)
  }

  @inline
  def post(task: Runnable): Unit = {
    progressThread.submit(task)
  }
  /**
   * Add executor's worker address. For standalone testing purpose and for implementations that makes
   * connection establishment outside of UcxShuffleManager.
   */
  override def addExecutor(executorId: ExecutorId, workerAddress: ByteBuffer): Unit = {
    executorAddresses.put(executorId, workerAddress)
    globalWapper.getConnection(executorId)
  }

  def addExecutors(executorIdsToAddress: Map[ExecutorId, SerializableDirectBuffer]): Unit = {
    executorIdsToAddress.foreach {
      case (executorId, address) => executorAddresses.put(executorId, address.value)
    }
  }

  def preConnect(): Unit = {
    globalWapper.preconnect()
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
    globalWapper.fetchBlocksByBlockIds(executorId, blockIds,
                                       resultBufferAllocator, callbacks)
  }

  def fetchBlockByStream(executorId: ExecutorId, blockId: BlockId,
                         resultBufferAllocator: BufferAllocator,
                         callback: OperationCallback): Unit = {
    globalWapper.fetchBlockByStream(executorId, blockId,
                                    resultBufferAllocator, callback)
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
