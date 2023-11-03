/*
 * Copyright (C) 2022, NVIDIA CORPORATION & AFFILIATES. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.memory.UcxHostBounceBuffersPool
import org.apache.spark.shuffle.ucx.rpc.GlobalWorkerRpcThread
import org.apache.spark.shuffle.ucx.utils.{SerializableDirectBuffer, SerializationUtils}
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.openucx.jucx.UcxException
import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants

import java.lang.ThreadLocal
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ExecutorService}
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class UcxRequest(private var request: UcpRequest, stats: OperationStats)
  extends Request {

  private[ucx] var completed = false

  override def isCompleted: Boolean = completed || ((request != null) && request.isCompleted)

  override def getStats: Option[OperationStats] = Some(stats)

  private[ucx] def setRequest(request: UcpRequest): Unit = {
    this.request = request
  }
}

class UcxStats extends OperationStats {
  private[ucx] val startTime = System.nanoTime()
  private[ucx] var amHandleTime = 0L
  private[ucx] var endTime: Long = 0L
  private[ucx] var receiveSize: Long = 0L

  /**
   * Time it took from operation submit to callback call.
   * This depends on [[ ShuffleTransport.progress() ]] calls,
   * and does not indicate actual data transfer time.
   */
  override def getElapsedTimeNs: Long = endTime - startTime

  /**
   * Indicates number of valid bytes in receive memory
   */
  override def recvSize: Long = receiveSize
}

case class UcxShuffleBockId(shuffleId: Int, mapId: Int, reduceId: Int) extends BlockId {
  override def serializedSize: Int = 12

  override def serialize(byteBuffer: ByteBuffer): Unit = {
    byteBuffer.putInt(shuffleId)
    byteBuffer.putInt(mapId)
    byteBuffer.putInt(reduceId)
  }
}

object UcxShuffleBockId {
  def deserialize(byteBuffer: ByteBuffer): UcxShuffleBockId = {
    val shuffleId = byteBuffer.getInt
    val mapId = byteBuffer.getInt
    val reduceId = byteBuffer.getInt
    UcxShuffleBockId(shuffleId, mapId, reduceId)
  }
}

class UcxShuffleTransport(var ucxShuffleConf: UcxShuffleConf = null, var executorId: Long = 0) extends ShuffleTransport
  with Logging {
  @volatile private var initialized: Boolean = false
  @volatile private var running: Boolean = true
  private[ucx] var ucxContext: UcpContext = _
  private var globalWorker: UcpWorker = _
  private var listener: UcpListener = _
  private val ucpWorkerParams = new UcpWorkerParams().requestThreadSafety()
  val endpoints = mutable.Set.empty[UcpEndpoint]
  val executorAddresses = new TrieMap[ExecutorId, ByteBuffer]
  
  private var allocatedClientWorkers: Array[UcxWorkerWrapper] = _
  private var clientWorkerId = new AtomicInteger()
  // private var clientLocal = new ThreadLocal[UcxWorkerWrapper] = _
  
  private var allocatedServerWorkers: Array[UcxWorkerWrapper] = _
  private val serverWorkerId = new AtomicInteger()
  
  private val registeredBlocks = new TrieMap[BlockId, Block]
  private var progressExecutors: ExecutorService = _
  private var replyExecutors: ExecutorService = _

  var hostBounceBufferMemoryPool: UcxHostBounceBuffersPool = _

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
    globalWorker = ucxContext.newWorker(ucpWorkerParams)
    hostBounceBufferMemoryPool = new UcxHostBounceBuffersPool(ucxShuffleConf, ucxContext)

    replyExecutors = Executors.newFixedThreadPool(ucxShuffleConf.numListenerThreads)
    // replyExecutors = Executors.newWorkStealingPool(ucxShuffleConf.numListenerThreads)
    progressExecutors = Executors.newFixedThreadPool(
      ucxShuffleConf.numListenerThreads + ucxShuffleConf.numWorkers + 1)

    allocatedServerWorkers = new Array[UcxWorkerWrapper](ucxShuffleConf.numListenerThreads)
    logInfo(s"Allocating ${ucxShuffleConf.numListenerThreads} server workers")
    for (i <- 0 until ucxShuffleConf.numListenerThreads) {
      val worker = ucxContext.newWorker(ucpWorkerParams)
      allocatedServerWorkers(i) = UcxWorkerWrapper(worker, this, isClientWorker = false, i.toLong)
      progressExecutors.submit(new ProgressTask(worker))
    }

    val Array(host, port) = ucxShuffleConf.listenerAddress.split(":")
    listener = globalWorker.newListener(new UcpListenerParams().setSockAddr(
      new InetSocketAddress(host, port.toInt))
      .setConnectionHandler((ucpConnectionRequest: UcpConnectionRequest) => {
        endpoints.add(globalWorker.newEndpoint(new UcpEndpointParams().setConnectionRequest(ucpConnectionRequest)
          .setPeerErrorHandlingMode().setErrorHandler(errorHandler)
          .setName(s"Endpoint to ${ucpConnectionRequest.getClientId}")))
      }))
    // Main RPC thread. Submit each RPC request to separate thread and send reply back from separate worker.
    globalWorker.setAmRecvHandler(0, (headerAddress: Long, headerSize: Long, amData: UcpAmData, _: UcpEndpoint) => {
      val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
      val replyTag = header.getInt
      val replyExecutor = header.getLong
      handleFetchBlockRequest(replyTag, amData, replyExecutor)
      UcsConstants.STATUS.UCS_INPROGRESS
    }, UcpConstants.UCP_AM_FLAG_PERSISTENT_DATA | UcpConstants.UCP_AM_FLAG_WHOLE_MSG )
    // AM to get worker address for client worker and connect server workers to it
    globalWorker.setAmRecvHandler(1, (headerAddress: Long, headerSize: Long, amData: UcpAmData,
                                      _: UcpEndpoint) => {
      val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
      val executorId = header.getLong
      val workerAddress = UnsafeUtils.getByteBufferView(amData.getDataAddress, amData.getLength.toInt)
      connectServerWorkers(executorId, workerAddress)
      UcsConstants.STATUS.UCS_OK
    }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG)
    progressExecutors.submit(new ProgressTask(globalWorker))

    allocatedClientWorkers = new Array[UcxWorkerWrapper](ucxShuffleConf.numWorkers)
    logInfo(s"Allocating ${ucxShuffleConf.numWorkers} client workers")
    for (i <- 0 until ucxShuffleConf.numWorkers) {
      val clientId: Long = ((i.toLong + 1L) << 32) | executorId
      ucpWorkerParams.setClientId(clientId)
      val worker = ucxContext.newWorker(ucpWorkerParams)
      allocatedClientWorkers(i) = UcxWorkerWrapper(worker, this, isClientWorker = true, clientId)
      progressExecutors.submit(new ProgressTask(worker))
    }

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

      hostBounceBufferMemoryPool.close()

      allocatedClientWorkers.foreach(_.close)

      if (listener != null) {
        listener.close()
        listener = null
      }

      if (globalWorker != null) {
        globalWorker.close()
        globalWorker = null
      }

      allocatedServerWorkers.foreach(_.close)
      running = false
      replyExecutors.shutdown()
      progressExecutors.shutdown()
      if (ucxContext != null) {
        ucxContext.close()
        ucxContext = null
      }
    }
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
    selectClientWorker.fetchBlocksByBlockIds(
        executorId, blockIds, resultBufferAllocator, callbacks)
  }

  def connectServerWorkers(executorId: ExecutorId, workerAddress: ByteBuffer): Unit = {
    allocatedServerWorkers.foreach(_.connectByWorkerAddress(executorId, workerAddress))
  }

  def handleFetchBlockRequest(replyTag: Int, amData: UcpAmData, replyExecutor: Long): Unit = {
    replyExecutors.submit(new Runnable {
      override def run = {
        val buffer = UnsafeUtils.getByteBufferView(amData.getDataAddress, amData.getLength.toInt)
        val blockIds = mutable.ArrayBuffer.empty[BlockId]

        // 1. Deserialize blockIds from header
        while (buffer.remaining() > 0) {
          val blockId = UcxShuffleBockId.deserialize(buffer)
          if (!registeredBlocks.contains(blockId)) {
            throw new UcxException(s"$blockId is not registered")
          }
          blockIds += blockId
        }
        amData.close()

        val blocks = blockIds.map(bid => registeredBlocks(bid))

        selectServerWorker.handleFetchBlockRequest(blocks, replyTag, replyExecutor)
      }
    })
  }

  // @inline
  // def selectClientWorker(): UcxWorkerWrapper = Option(clientLocal.get) match {
  //   case Some(client) => client
  //   case None => 
  //     val client = allocatedClientWorkers(
  //       (clientWorkerId.incrementAndGet() % allocatedClientWorkers.length).abs)
  //     clientLocal.set(client)
  //     client
  // }

  @inline
  def selectClientWorker(): UcxWorkerWrapper = allocatedClientWorkers(
    (clientWorkerId.incrementAndGet() % allocatedClientWorkers.length).abs)

  @inline
  def selectServerWorker(): UcxWorkerWrapper = allocatedServerWorkers(
    (serverWorkerId.incrementAndGet() % allocatedServerWorkers.length).abs)

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
