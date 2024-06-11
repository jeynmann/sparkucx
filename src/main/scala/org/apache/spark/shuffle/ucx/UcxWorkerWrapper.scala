/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import java.io.Closeable
import java.util.concurrent.{ConcurrentLinkedQueue, Semaphore, Callable, Future, FutureTask}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap
import scala.util.Random
import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants
import org.openucx.jucx.ucs.UcsConstants.MEMORY_TYPE
import org.openucx.jucx.{UcxCallback, UcxException, UcxUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.memory.UcxLinkedMemBlock
import org.apache.spark.shuffle.ucx.memory.UcxSharedMemoryBlock
import org.apache.spark.shuffle.ucx.utils.SerializationUtils
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.ThreadUtils

import java.nio.ByteBuffer
import scala.collection.parallel.ForkJoinTaskSupport

private[ucx] class UcxSucceedOperationResult(
  mem: MemoryBlock, stats: OperationStats) extends OperationResult {
  override def getStatus: OperationStatus.Value = OperationStatus.SUCCESS

  override def getError: TransportError = null

  override def getStats: Option[OperationStats] = Option(stats)

  override def getData: MemoryBlock = mem
}

class UcxFailureOperationResult(errorMsg: String) extends OperationResult {
  override def getStatus: OperationStatus.Value = OperationStatus.FAILURE

  override def getError: TransportError = new TransportError(errorMsg)

  override def getStats: Option[OperationStats] = None

  override def getData: MemoryBlock = null
}

class UcxAmDataMemoryBlock(ucpAmData: UcpAmData, offset: Long, size: Long,
                           refCount: AtomicInteger)
  extends MemoryBlock(ucpAmData.getDataAddress + offset, size, true) with Logging {

  override def close(): Unit = {
    if (refCount.decrementAndGet() == 0) {
      ucpAmData.close()
    }
  }
}

class UcxRefCountMemoryBlock(baseBlock: MemoryBlock, offset: Long, size: Long,
                             refCount: AtomicInteger)
  extends MemoryBlock(baseBlock.address + offset, size, true) with Logging {

  override def close(): Unit = {
    if (refCount.decrementAndGet() == 0) {
      baseBlock.close()
    }
  }
}

/**
 * Worker per thread wrapper, that maintains connection and progress logic.
 */
case class UcxWorkerWrapper(worker: UcpWorker, transport: UcxShuffleTransport, isClientWorker: Boolean,
                            id: Long = 0L)
  extends Closeable with Logging {
  private[ucx] final val timeout = transport.ucxShuffleConf.getSparkConf.getTimeAsSeconds(
    "spark.network.timeout", "120s") * 1000
  private[ucx] final val connections = new TrieMap[transport.ExecutorId, UcpEndpoint]
  private[ucx] lazy val requestData = new TrieMap[Int, UcxFetchState]
  private[ucx] lazy val streamData = new TrieMap[Int, UcxStreamState]
  private[ucx] lazy val sliceData = new TrieMap[Int, UcxSliceState]
  private[ucx] lazy val tag = new AtomicInteger(Random.nextInt())

  private[ucx] lazy val ioThreadOn = transport.ucxShuffleConf.numIoThreads > 1
  private[ucx] lazy val ioThreadPool = ThreadUtils.newForkJoinPool("IO threads",
    transport.ucxShuffleConf.numIoThreads)
  private[ucx] lazy val ioTaskSupport = new ForkJoinTaskSupport(ioThreadPool)
  private[ucx] var progressThread: Thread = _

  private[ucx] lazy val maxReplySize = transport.getMaxReplySize()
  private[ucx] lazy val memPool = if (isClientWorker) {
    transport.hostBounceBufferMemoryPool
  } else {
    transport.serverBounceBufferMemoryPool
  }

  private[this] case class UcxSliceReplyHandle() extends UcpAmRecvCallback() {
    override def onReceive(headerAddress: Long, headerSize: Long,
                           ucpAmData: UcpAmData, ep: UcpEndpoint): Int = {
      assert(!ucpAmData.isDataValid)
      val headerBuffer = UnsafeUtils.getByteBufferView(headerAddress,
                                                       headerSize.toInt)
      val i = headerBuffer.getInt
      val remaining = headerBuffer.getInt
      val length = headerBuffer.getLong
      val offset = headerBuffer.getLong

      val sliceState = sliceData.getOrElseUpdate(i, {
        requestData.get(i) match {
          case Some(data) => {
            val mem = memPool.get(length)
            val memRef = new UcxRefCountMemoryBlock(mem, 0, length,
                                                    new AtomicInteger(1))
            new UcxSliceState(data.callbacks(0), data.request, data.timestamp,
                              memRef, length)
          }
          case None => throw new UcxException(s"Slice tag $i context not found.")
        }
      })

      val stats = sliceState.request.getStats.get.asInstanceOf[UcxStats]
      stats.receiveSize += ucpAmData.getLength
      stats.amHandleTime = System.nanoTime()

      val currentAddress = sliceState.mem.address + offset
      worker.recvAmDataNonBlocking(
        ucpAmData.getDataHandle, currentAddress, ucpAmData.getLength,
        new UcxCallback() {
          override def onSuccess(r: UcpRequest): Unit = {
            stats.endTime = System.nanoTime()
            logDebug(s"Slice receive rndv data size ${ucpAmData.getLength} " +
              s"tag $i ($offset, $length) in ${stats.getElapsedTimeNs} ns " +
              s"amHandle ${stats.endTime - stats.amHandleTime} ns")
            receivedSlice(i, offset, length, ucpAmData.getLength, sliceState)
          }
        }, UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
      UcsConstants.STATUS.UCS_OK
    }
  }

  private[this] case class UcxStreamReplyHandle() extends UcpAmRecvCallback() {
    override def onReceive(headerAddress: Long, headerSize: Long,
                           ucpAmData: UcpAmData, ep: UcpEndpoint): Int = {
      assert(!ucpAmData.isDataValid)
      val headerBuffer = UnsafeUtils.getByteBufferView(headerAddress,
                                                       headerSize.toInt)
      val i = headerBuffer.getInt
      val remaining = headerBuffer.getInt
      val length = headerBuffer.getLong
      val offset = headerBuffer.getLong

      val mem = memPool.get(ucpAmData.getLength)
      val memRef = new UcxRefCountMemoryBlock(mem, 0, ucpAmData.getLength,
                                              new AtomicInteger(1))

      val data = streamData.get(i)
      if (data.isEmpty) {
        throw new UcxException(s"Stream tag $i context not found.")
      }

      val streamState = data.get
      if (streamState.remaining == Long.MaxValue) {
        streamState.remaining = length
      }

      val stats = streamState.request.getStats.get.asInstanceOf[UcxStats]
      stats.receiveSize += ucpAmData.getLength
      stats.amHandleTime = System.nanoTime()

      worker.recvAmDataNonBlocking(
        ucpAmData.getDataHandle, mem.address, ucpAmData.getLength,
        new UcxCallback() {
          override def onSuccess(r: UcpRequest): Unit = {
            stats.endTime = System.nanoTime()
            logDebug(s"Stream receive rndv data size ${ucpAmData.getLength} " +
              s"tag $i ($offset, $length) in ${stats.getElapsedTimeNs} ns " +
              s"amHandle ${stats.endTime - stats.amHandleTime} ns")
            receivedStream(i, offset, length, memRef, streamState)
          }
        }, UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
      UcsConstants.STATUS.UCS_OK
    }
  }

  if (isClientWorker) {
    worker.setAmRecvHandler(3, UcxSliceReplyHandle(), UcpConstants.UCP_AM_FLAG_WHOLE_MSG)
    worker.setAmRecvHandler(2, UcxStreamReplyHandle(), UcpConstants.UCP_AM_FLAG_WHOLE_MSG)
    // Receive block data handler
    worker.setAmRecvHandler(1,
      (headerAddress: Long, headerSize: Long, ucpAmData: UcpAmData, _: UcpEndpoint) => {
        val headerBuffer = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
        val i = headerBuffer.getInt
        val data = requestData.get(i)

        if (data.isEmpty) {
          throw new UcxException(s"No data for tag $i.")
        }

        val fetchState = data.get
        val callbacks = fetchState.callbacks
        val request = fetchState.request
        val stats = request.getStats.get.asInstanceOf[UcxStats]
        stats.receiveSize = ucpAmData.getLength

        // Header contains tag followed by sizes of blocks
        val numBlocks = (headerSize.toInt - UnsafeUtils.INT_SIZE) / UnsafeUtils.INT_SIZE

        var offset = 0
        val refCounts = new AtomicInteger(numBlocks)
        if (ucpAmData.isDataValid) {
          request.completed = true
          stats.endTime = System.nanoTime()
          logDebug(s"Received amData: $ucpAmData for tag $i " +
            s"in ${stats.getElapsedTimeNs} ns")

          val closeCb = () => worker.synchronized { ucpAmData.close() }
          val address = ucpAmData.getDataAddress
          for (b <- 0 until numBlocks) {
            val blockSize = headerBuffer.getInt
            if (callbacks(b) != null) {
              val mem = new UcxSharedMemoryBlock(closeCb, refCounts, address + offset,
                                                 blockSize)
              receivedChunk(i, b, mem, fetchState)
              offset += blockSize
            }
          }
          if (callbacks.isEmpty) UcsConstants.STATUS.UCS_OK else UcsConstants.STATUS.UCS_INPROGRESS
        } else {
          val mem = memPool.get(ucpAmData.getLength)
          stats.amHandleTime = System.nanoTime()
          request.setRequest(worker.recvAmDataNonBlocking(ucpAmData.getDataHandle, mem.address, ucpAmData.getLength,
            new UcxCallback() {
              override def onSuccess(r: UcpRequest): Unit = {
                request.completed = true
                stats.endTime = System.nanoTime()
                logDebug(s"Received rndv data of size: ${ucpAmData.getLength}" +
                  s" for tag $i in ${stats.getElapsedTimeNs} ns " +
                  s"time from amHandle: ${System.nanoTime() - stats.amHandleTime} ns")
                for (b <- 0 until numBlocks) {
                  val blockSize = headerBuffer.getInt
                  val memRef = new UcxRefCountMemoryBlock(mem, offset, blockSize, refCounts)
                  receivedChunk(i, b, memRef, fetchState)
                  offset += blockSize
                }

              }
            }, UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST))
          UcsConstants.STATUS.UCS_OK
        }
      }, UcpConstants.UCP_AM_FLAG_PERSISTENT_DATA | UcpConstants.UCP_AM_FLAG_WHOLE_MSG)
  }
  private def receivedSlice(tag: Int, offset: Long, length: Long, msgLen: Long,
                            sliceState: UcxSliceState): Unit = {
    if (!sliceState.recvSet.add(offset)) {
      logWarning(s"Received duplicate slice: tag $tag offset $offset")
      return
    }

    logTrace(s"tag $tag $sliceState")
    sliceState.remaining -= msgLen
    if (sliceState.remaining != 0) {
      return
    }

    val stats = sliceState.request.getStats.get
    val result = new UcxSucceedOperationResult(sliceState.mem, stats)
    sliceState.callback.onComplete(result)
    sliceData.remove(tag)

    val data = requestData.get(tag)
    if (data.isEmpty) {
      logWarning(s"No fetch found: tag $tag")
      return
    }
    // block must be the last chunk
    val fetchState = data.get
    val chunkId = fetchState.callbacks.size - 1
    receivedChunk(tag, chunkId, null, fetchState)
  }

  private def receivedStream(tag: Int, offset: Long, length: Long, mem: MemoryBlock,
                             streamState: UcxStreamState): Unit = {
    if (streamState.recvMap.put(offset, mem) != null) {
      logWarning(s"Received duplicate stream: tag $tag offset $offset")
      return
    }

    logTrace(s"tag $tag $streamState")
    var received = length - streamState.remaining
    var memNow = streamState.recvMap.get(received)
    while (memNow != null) {
      val buf = UnsafeUtils.getByteBufferView(memNow.address, memNow.size.toInt)
      streamState.callback.onData(buf)
      received += memNow.size
      memNow.close()
      memNow = streamState.recvMap.get(received)
    }

    streamState.remaining = length - received
    if (streamState.remaining != 0) {
      return
    }

    val stats = streamState.request.getStats.get
    val result = new UcxSucceedOperationResult(null, stats)
    streamState.callback.onComplete(result)
    streamData.remove(tag)
  }

  private def receivedChunk(tag: Int, chunkId: Int, mem: MemoryBlock,
                            fetchState: UcxFetchState): Unit = {
    if (!fetchState.recvSet.add(chunkId)) {
      logWarning(s"Received duplicate chunk: tag $tag chunk $chunkId")
      return
    }

    if (mem != null) {
      val stats = fetchState.request.getStats.get
      val result = new UcxSucceedOperationResult(mem, stats)
      fetchState.callbacks(chunkId).onComplete(result)
    }

    logTrace(s"tag $tag $fetchState")
    if (fetchState.recvSet.size != fetchState.callbacks.size) {
      return
    }

    requestData.remove(tag)
  }

  override def close(): Unit = {
    if (isClientWorker) {
      val closeRequests = connections.map {
        case (_, endpoint) => endpoint.closeNonBlockingForce()
      }
      while (!closeRequests.forall(_.isCompleted)) {
        progress()
      }
    }
    connections.clear()
    if (progressThread != null) {
      progressThread.interrupt()
      progressThread.join(1)
    }
    if (ioThreadOn) {
      ioThreadPool.shutdown()
    }
    worker.close()
  }

  def progressStart(): Unit = {
    progressThread = new ProgressThread(s"UCX-progress-$id", worker,
                                        transport.ucxShuffleConf.useWakeup)
    progressThread.start()
  }

  /**
   * Blocking progress until there's outstanding flush requests.
   */
  def progressConnect(): Unit = {
  }

  /**
   * The only place for worker progress
   */
  def progress(): Int = worker.synchronized {
    worker.progress()
  }

  /**
   * Establish connections to known instances.
   */
  def preconnect(): Unit = {
    transport.executorAddresses.keys.foreach(getConnection)
    progressConnect()
  }

  def connectByWorkerAddress(executorId: transport.ExecutorId, workerAddress: ByteBuffer): Unit = {
    logDebug(s"Worker $this connecting back to $executorId by worker address")
    val ep = worker.synchronized {
      worker.newEndpoint(new UcpEndpointParams().setName(s"Server connection to $executorId")
        .setUcpAddress(workerAddress))
    }
    connections.put(executorId, ep)
  }

  def getConnection(executorId: transport.ExecutorId): UcpEndpoint = {
    if (!connections.contains(executorId)) {
      if (!transport.executorAddresses.contains(executorId)) {
        val startTime = System.currentTimeMillis()
        while (!transport.executorAddresses.contains(executorId)) {
          if  (System.currentTimeMillis() - startTime > timeout) {
            throw new UcxException(s"Don't get a worker address for $executorId")
          }
        }
        val cost = System.currentTimeMillis() - startTime
        if (cost > 1000) {
          logInfo(s"@D get ${executorId} ${cost} ms")
        }
      }
    }

    connections.getOrElseUpdate(executorId, {
      val address = transport.executorAddresses(executorId)
      val endpointParams = new UcpEndpointParams().setPeerErrorHandlingMode()
        .setUcpAddress(address).sendClientId()
        .setErrorHandler(new UcpEndpointErrorHandler() {
          override def onError(ep: UcpEndpoint, status: Int, errorMsg: String): Unit = {
            logError(s"Endpoint to $executorId got an error: $errorMsg")
            connections.remove(executorId)
          }
        }).setName(s"Endpoint to $executorId")

      logDebug(s"@D Worker ${id.toInt}:${id>>32} connecting to Executor($executorId)")
      val header = Platform.allocateDirectBuffer(UnsafeUtils.LONG_SIZE)
      header.putLong(id)
      header.rewind()
      val workerAddress = worker.getAddress

      worker.synchronized {
        val ep = worker.newEndpoint(endpointParams)
        ep.sendAmNonBlocking(1, UcxUtils.getAddress(header), UnsafeUtils.LONG_SIZE,
          UcxUtils.getAddress(workerAddress), workerAddress.capacity().toLong, UcpConstants.UCP_AM_SEND_FLAG_EAGER,
          new UcxCallback() {
            override def onSuccess(request: UcpRequest): Unit = {
              header.clear()
              workerAddress.clear()
            }
            override def onError(ucsStatus: Int, errorMsg: String): Unit = {
              logError(s"Failed to send $errorMsg")
            }
          }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
        ep
      }
    })
  }

  def fetchBlocksByBlockIds(executorId: transport.ExecutorId, blockIds: Seq[BlockId],
                            resultBufferAllocator: transport.BufferAllocator,
                            callbacks: Seq[OperationCallback]): Unit = {
    val startTime = System.nanoTime()
    val headerSize = UnsafeUtils.INT_SIZE + UnsafeUtils.LONG_SIZE
    
    val t = tag.incrementAndGet()

    val buffer = Platform.allocateDirectBuffer(headerSize + blockIds.map(_.serializedSize).sum)
    buffer.putInt(t)
    buffer.putLong(id)
    blockIds.foreach(b => b.serialize(buffer))

    val request = new UcxRequest(null, new UcxStats())
    requestData.put(t, new UcxFetchState(callbacks, request, startTime))

    buffer.rewind()
    val address = UnsafeUtils.getAdress(buffer)
    val dataAddress = address + headerSize

    val ep = getConnection(executorId)
    worker.synchronized {
      ep.sendAmNonBlocking(0, address,
        headerSize, dataAddress, buffer.capacity() - headerSize,
        UcpConstants.UCP_AM_SEND_FLAG_EAGER, new UcxCallback() {
        override def onSuccess(request: UcpRequest): Unit = {
          buffer.clear()
          logDebug(s"Sent message on $ep to $executorId to fetch ${blockIds.length} blocks on tag $t id $id" +
            s"in ${System.nanoTime() - startTime} ns")
        }
      }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
    }
  }

  def handleFetchBlockRequest(blocks: Seq[Block], replyTag: Int,
                              replyExecutor: Long): Unit = {
    val blockSize = blocks.map(_.getSize).sum
    if (blockSize <= maxReplySize) {
      handleFetchBlockChunks(blocks, blockSize, replyTag, replyExecutor)
      return
    }
    // The size of last block could > maxBytesInFlight / 5 in spark.
    val lastBlock = blocks.last
    if (blocks.size > 1) {
      val chunks = blocks.slice(0, blocks.size - 1)
      val chunksSize = blockSize - lastBlock.getSize
      handleFetchBlockChunks(chunks, chunksSize, replyTag, replyExecutor)
    }
    handleFetchBlockStream(lastBlock, replyTag, replyExecutor, 3)
  }

  def handleFetchBlockChunks(blocks: Seq[Block], blockSize: Long, replyTag: Int,
                             replyExecutor: Long): Unit = try {
    val tagAndSizes = UnsafeUtils.INT_SIZE + UnsafeUtils.INT_SIZE * blocks.length
    val msgSize = tagAndSizes + blockSize
    val resultMemory = memPool.get(msgSize).asInstanceOf[UcxLinkedMemBlock]
    assert(resultMemory != null)
    val resultBuffer = UcxUtils.getByteBufferView(resultMemory.address, msgSize)
    resultBuffer.putInt(replyTag)

    var offset = 0
    val localBuffers = blocks.zipWithIndex.map {
      case (block, i) =>
        resultBuffer.putInt(UnsafeUtils.INT_SIZE + i * UnsafeUtils.INT_SIZE, block.getSize.toInt)
        resultBuffer.position(tagAndSizes + offset)
        val localBuffer = resultBuffer.slice()
        offset += block.getSize.toInt
        localBuffer.limit(block.getSize.toInt)
        localBuffer
    }
    // Do parallel read of blocks
    val blocksCollection = if (ioThreadOn) {
      val parCollection = blocks.indices.par
      parCollection.tasksupport = ioTaskSupport
      parCollection
    } else {
      blocks.indices
    }

    val readTime = System.nanoTime()

    for (i <- blocksCollection) {
      blocks(i).getBlock(localBuffers(i), 0)
    }

    val startTime = System.nanoTime()
    val ep = connections(replyExecutor)
    worker.synchronized {
      ep.sendAmNonBlocking(1, resultMemory.address, tagAndSizes,
        resultMemory.address + tagAndSizes, msgSize - tagAndSizes,
        0, new UcxCallback {
          override def onSuccess(request: UcpRequest): Unit = {
            logTrace(s"Sent ${blocks.length} blocks of size: ${msgSize} " +
              s"to tag $replyTag in ${System.nanoTime() - startTime} ns read " +
              s"${startTime - readTime} ns")
            resultMemory.close()
          }
          override def onError(ucsStatus: Int, errorMsg: String): Unit = {
            logError(s"Failed to send $errorMsg")
            resultMemory.close()
          }
        }, new UcpRequestParams().setMemoryType(UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
          .setMemoryHandle(resultMemory.memory))
    }
  } catch {
    case ex: Throwable => logError(s"Failed to read and send data: $ex")
  }

  def fetchBlockByStream(executorId: transport.ExecutorId, blockId: BlockId,
                         resultBufferAllocator: transport.BufferAllocator,
                         callback: OperationCallback): Unit = {
    val startTime = System.nanoTime()
    val headerSize = UnsafeUtils.INT_SIZE + UnsafeUtils.LONG_SIZE +
                     blockId.serializedSize

    val t = tag.incrementAndGet()

    val buffer = Platform.allocateDirectBuffer(headerSize)
    buffer.putInt(t)
    buffer.putLong(id)
    blockId.serialize(buffer)

    val request = new UcxRequest(null, new UcxStats())
    streamData.put(t, new UcxStreamState(callback, request, startTime,
                   Long.MaxValue))

    val address = UnsafeUtils.getAdress(buffer)

    val ep = getConnection(executorId)
    worker.synchronized {
      ep.sendAmNonBlocking(2, address, headerSize, address, 0,
        UcpConstants.UCP_AM_SEND_FLAG_EAGER, new UcxCallback() {
        override def onSuccess(request: UcpRequest): Unit = {
          buffer.clear()
          logDebug(s"Worker $id sent stream to $executorId block $blockId " +
            s"tag $t in ${System.nanoTime() - startTime} ns")
        }
      }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
    }
  }

  def handleFetchBlockStream(block: Block, replyTag: Int,
                             replyExecutor: Long, amId: Int = 2): Unit = {
    // tag: Int + unsent replies: Int + total length: Long + offset now: Long
    val headerSize = UnsafeUtils.INT_SIZE + UnsafeUtils.INT_SIZE +
                     UnsafeUtils.LONG_SIZE + UnsafeUtils.LONG_SIZE
    val blockSize = block.getSize
    val blockSlice = (0L until blockSize by maxReplySize).toArray
    // make sure the last one is not too small
    if (blockSlice.size >= 2) {
      val mid = (blockSlice(blockSlice.size - 2) + blockSize) / 2
      blockSlice(blockSlice.size - 1) = mid
    }
    val sem = new Semaphore(1) // max use 2 buffers.

    def send(workerWrapper: UcxWorkerWrapper, currentId: Int): Unit = try {
      val hashNext = (currentId + 1 != blockSlice.size)
      val nextOffset = if (hashNext) blockSlice(currentId + 1) else blockSize
      val currentOffset = blockSlice(currentId)
      val currentSize = nextOffset - currentOffset
      val unsent = blockSlice.length - currentId - 1
      val msgSize = headerSize + currentSize.toInt
      val mem = memPool.get(msgSize).asInstanceOf[UcxLinkedMemBlock]
      val buffer = mem.toByteBuffer()

      buffer.limit(msgSize)
      buffer.putInt(replyTag)
      buffer.putInt(unsent)
      buffer.putLong(blockSize)
      buffer.putLong(currentOffset)
      block.getBlock(buffer, currentOffset)

      val startTime = System.nanoTime()
      val ep = workerWrapper.connections(replyExecutor)
      workerWrapper.worker.synchronized {
        ep.sendAmNonBlocking(amId, mem.address, headerSize,
          mem.address + headerSize, currentSize,
          UcpConstants.UCP_AM_SEND_FLAG_RNDV, new UcxCallback {
            override def onSuccess(request: UcpRequest): Unit = {
              logTrace(s"Reply stream block $currentId size $currentSize tag " +
                s"$replyTag in ${System.nanoTime() - startTime} ns.")
                mem.close()
                sem.release(1)
            }
            override def onError(ucsStatus: Int, errorMsg: String): Unit = {
              logError(s"Failed to reply stream $errorMsg")
              mem.close()
              sem.release(1)
            }
          }, new UcpRequestParams()
            .setMemoryType(UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
            .setMemoryHandle(mem.memory))
      }
      if (hashNext) {
        sem.acquire(1)
        transport.replyThreadPool.submit(new Runnable {
          override def run = send(transport.selectServerWorker, currentId + 1)
        })
      }
    } catch {
      case ex: Throwable => logError(s"Failed to reply stream $ex.")
    }

    send(this, 0)
  }

}

private[ucx] class ProgressThread(
  name: String, worker: UcpWorker, useWakeup: Boolean) extends Thread {
  setDaemon(true)
  setName(name)

  override def run(): Unit = {
    while (!isInterrupted) {
      worker.synchronized {
        while (worker.progress != 0) {}
      }
      if (useWakeup) {
        worker.waitForEvents()
      }
    }
  }
}