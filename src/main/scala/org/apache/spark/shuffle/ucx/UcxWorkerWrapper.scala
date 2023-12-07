/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import java.io.Closeable
import java.util.concurrent.{ConcurrentLinkedQueue, Callable, Future, FutureTask}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap
import scala.util.Random
import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants
import org.openucx.jucx.ucs.UcsConstants.MEMORY_TYPE
import org.openucx.jucx.{UcxCallback, UcxException, UcxUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.memory.UcxBounceBufferMemoryBlock
import org.apache.spark.shuffle.ucx.utils.SerializationUtils
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.ThreadUtils

import java.nio.ByteBuffer
import scala.collection.parallel.ForkJoinTaskSupport


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

private[ucx] class UcxStreamState(val callback: OperationCallback,
                                  val request: UcxRequest,
                                  var remaining: Int) {}

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

/**
 * Worker per thread wrapper, that maintains connection and progress logic.
 */
case class UcxWorkerWrapper(worker: UcpWorker, transport: UcxShuffleTransport, isClientWorker: Boolean,
                            id: Long = 0L)
  extends Closeable with Logging {
  private[ucx] final val timeout = transport.ucxShuffleConf.getSparkConf.getTimeAsMs("spark.network.timeout", "100")
  private[ucx] final val connections = new TrieMap[transport.ExecutorId, UcpEndpoint]
  private[ucx] lazy val requestData = new TrieMap[Int, (Seq[OperationCallback], UcxRequest, transport.BufferAllocator)]
  private[ucx] lazy val streamData = new TrieMap[Int, UcxStreamState]
  private[ucx] lazy val tag = new AtomicInteger(Random.nextInt())

  private[ucx] lazy val ioThreadOn = transport.ucxShuffleConf.numIoThreads > 1
  private[ucx] lazy val ioThreadPool = ThreadUtils.newForkJoinPool("IO threads",
    transport.ucxShuffleConf.numIoThreads)
  private[ucx] lazy val ioTaskSupport = new ForkJoinTaskSupport(ioThreadPool)
  private[ucx] var progressThread: Thread = _

  private[ucx] lazy val memPool = transport.hostBounceBufferMemoryPool
  private[ucx] lazy val maxReplySize = transport.ucxShuffleConf.maxReplySize

  private[this] case class UcxStreamReplyHandle() extends UcpAmRecvCallback() {
    override def onReceive(
      headerAddress: Long, headerSize: Long, ucpAmData: UcpAmData,
      ep: UcpEndpoint): Int = {
        val headerBuffer = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
        val i = headerBuffer.getInt
        val remaining = headerBuffer.getInt

        val data = streamData.get(i)
        if (data.isEmpty) {
          throw new UcxException(s"Stream tag $i context not found.")
        }

        val streamState = data.get
        if (remaining >= streamState.remaining) {
          throw new UcxException(
            s"Stream tag $i out of order $remaining <= ${streamState.remaining}.")
        }
        streamState.remaining = remaining

        val stats = streamState.request.getStats.get.asInstanceOf[UcxStats]
        stats.receiveSize += ucpAmData.getLength

        val refCounts = new AtomicInteger(1)
        if (ucpAmData.isDataValid) {
          stats.endTime = System.nanoTime()
          logDebug(s"Stream receive amData ${ucpAmData} tag $i in "
            + s"${stats.getElapsedTimeNs} ns")
          val buffer = UnsafeUtils.getByteBufferView(
            ucpAmData.getDataAddress, ucpAmData.getLength.toInt)
          streamState.callback.onData(buffer)
          if (remaining == 0) {
            streamState.callback.onComplete(new OperationResult {
              override def getStatus: OperationStatus.Value = OperationStatus.SUCCESS
              override def getError: TransportError = null
              override def getStats: Option[OperationStats] = Some(stats)
              override def getData: MemoryBlock = null
            })
            streamData.remove(i)
          }
        } else {
          val mem = memPool.get(ucpAmData.getLength)
          stats.amHandleTime = System.nanoTime()
          worker.recvAmDataNonBlocking(
            ucpAmData.getDataHandle, mem.address, ucpAmData.getLength,
            new UcxCallback() {
              override def onSuccess(r: UcpRequest): Unit = {
                stats.endTime = System.nanoTime()
                logDebug(s"Stream receive rndv data size ${mem.size} tag $i in " +
                  s"${stats.getElapsedTimeNs} ns " +
                  s" amHandle ${stats.endTime - stats.amHandleTime} ns")
                val buffer = UnsafeUtils.getByteBufferView(
                  mem.address, ucpAmData.getLength.toInt)
                streamState.callback.onData(buffer)
                mem.close()
                if (remaining == 0) {
                  streamState.callback.onComplete(new OperationResult {
                    override def getStatus: OperationStatus.Value = OperationStatus.SUCCESS
                    override def getError: TransportError = null
                    override def getStats: Option[OperationStats] = Some(stats)
                    override def getData: MemoryBlock = null
                  })
                  streamData.remove(i)
                }
              }
            }, UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
        }
        UcsConstants.STATUS.UCS_OK
      }
  }

  if (isClientWorker) {
    worker.setAmRecvHandler(2, UcxStreamReplyHandle(), UcpConstants.UCP_AM_FLAG_WHOLE_MSG)
    // Receive block data handler
    worker.setAmRecvHandler(1,
      (headerAddress: Long, headerSize: Long, ucpAmData: UcpAmData, _: UcpEndpoint) => {
        val headerBuffer = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
        val i = headerBuffer.getInt
        val data = requestData.remove(i)

        if (data.isEmpty) {
          throw new UcxException(s"No data for tag $i.")
        }

        val (callbacks, request, allocator) = data.get
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

          for (b <- 0 until numBlocks) {
            val blockSize = headerBuffer.getInt
            if (callbacks(b) != null) {
              callbacks(b).onComplete(new OperationResult {
                override def getStatus: OperationStatus.Value = OperationStatus.SUCCESS

                override def getError: TransportError = null

                override def getStats: Option[OperationStats] = Some(stats)

                override def getData: MemoryBlock = new UcxAmDataMemoryBlock(ucpAmData, offset, blockSize, refCounts)
              })
              offset += blockSize
            }
          }
          if (callbacks.isEmpty) UcsConstants.STATUS.UCS_OK else UcsConstants.STATUS.UCS_INPROGRESS
        } else {
          val mem = allocator(ucpAmData.getLength)
          stats.amHandleTime = System.nanoTime()
          request.setRequest(worker.recvAmDataNonBlocking(ucpAmData.getDataHandle, mem.address, ucpAmData.getLength,
            new UcxCallback() {
              override def onSuccess(r: UcpRequest): Unit = {
                request.completed = true
                stats.endTime = System.nanoTime()
                logDebug(s"Received rndv data of size: ${mem.size} for tag $i in " +
                  s"${stats.getElapsedTimeNs} ns " +
                  s"time from amHandle: ${System.nanoTime() - stats.amHandleTime} ns")
                for (b <- 0 until numBlocks) {
                  val blockSize = headerBuffer.getInt
                  callbacks(b).onComplete(new OperationResult {
                    override def getStatus: OperationStatus.Value = OperationStatus.SUCCESS

                    override def getError: TransportError = null

                    override def getStats: Option[OperationStats] = Some(stats)

                    override def getData: MemoryBlock = new UcxRefCountMemoryBlock(mem, offset, blockSize, refCounts)
                  })
                  offset += blockSize
                }

              }
            }, UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST))
          UcsConstants.STATUS.UCS_OK
        }
      }, UcpConstants.UCP_AM_FLAG_PERSISTENT_DATA | UcpConstants.UCP_AM_FLAG_WHOLE_MSG)
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
      val inetAddress = SerializationUtils.deserializeInetAddress(address)
      val endpointParams = new UcpEndpointParams().setPeerErrorHandlingMode()
        .setSocketAddress(inetAddress).sendClientId()
        .setErrorHandler(new UcpEndpointErrorHandler() {
          override def onError(ep: UcpEndpoint, status: Int, errorMsg: String): Unit = {
            logError(s"Endpoint to $executorId got an error: $errorMsg")
            connections.remove(executorId)
          }
        }).setName(s"Endpoint to $executorId")

      logDebug(s"@D Worker ${id.toInt}:${id>>32} connecting to Executor($executorId -> $inetAddress)")
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
    requestData.put(t, (callbacks, request, resultBufferAllocator))

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

  def handleFetchBlockRequest(blocks: Seq[Block], replyTag: Int, replyExecutor: Long): Unit = try {
    val tagAndSizes = UnsafeUtils.INT_SIZE + UnsafeUtils.INT_SIZE * blocks.length
    val resultMemory = transport.hostBounceBufferMemoryPool.get(tagAndSizes + blocks.map(_.getSize).sum)
      .asInstanceOf[UcxBounceBufferMemoryBlock]
    val resultBuffer = UcxUtils.getByteBufferView(resultMemory.address,
      resultMemory.size)
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

    for (i <- blocksCollection) {
      blocks(i).getBlock(localBuffers(i), 0)
    }

    val startTime = System.nanoTime()
    val ep = connections(replyExecutor)
    worker.synchronized {
      ep.sendAmNonBlocking(1, resultMemory.address, tagAndSizes,
        resultMemory.address + tagAndSizes, resultMemory.size - tagAndSizes,
        0, new UcxCallback {
          override def onSuccess(request: UcpRequest): Unit = {
            logTrace(s"Sent ${blocks.length} blocks of size: ${resultMemory.size} " +
              s"to tag $replyTag in ${System.nanoTime() - startTime} ns.")
            transport.hostBounceBufferMemoryPool.put(resultMemory)
          }
          override def onError(ucsStatus: Int, errorMsg: String): Unit = {
            logError(s"Failed to send $errorMsg")
          }
        }, new UcpRequestParams().setMemoryType(UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
          .setMemoryHandle(resultMemory.memory))
    }
  } catch {
    case ex: Throwable => logError(s"Failed to read and send data: $ex")
  }

  def fetchBlocksByStream(executorId: transport.ExecutorId, blockId: BlockId,
                            resultBufferAllocator: transport.BufferAllocator,
                            callback: OperationCallback): Unit = {
    val startTime = System.nanoTime()
    val headerSize = UnsafeUtils.INT_SIZE + UnsafeUtils.LONG_SIZE + blockId.serializedSize

    val t = tag.incrementAndGet()

    val buffer = Platform.allocateDirectBuffer(headerSize)
    buffer.putInt(t)
    buffer.putLong(id)
    blockId.serialize(buffer)

    val request = new UcxRequest(null, new UcxStats())
    streamData.put(t, new UcxStreamState(callback, request, Int.MaxValue))

    val address = UnsafeUtils.getAdress(buffer)

    val ep = getConnection(executorId)
    worker.synchronized {
      ep.sendAmNonBlocking(2, address, headerSize, address, 0,
        UcpConstants.UCP_AM_SEND_FLAG_EAGER, new UcxCallback() {
        override def onSuccess(request: UcpRequest): Unit = {
          buffer.clear()
          logDebug(s"Worker $id sent stream to $executorId block ${blockId} tag $t" +
            s"in ${System.nanoTime() - startTime} ns")
        }
      }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
    }
  }

  def handleFetchBlockStream(block: Block, replyTag: Int, replyExecutor: Long): Unit = {
    val headerSize = UnsafeUtils.INT_SIZE + UnsafeUtils.INT_SIZE
    val maxBodySize = maxReplySize - headerSize.toLong
    val blockSize = block.getSize
    val blockSlice = (0L until blockSize by maxBodySize)

    val mem = memPool.get(maxReplySize).asInstanceOf[UcxBounceBufferMemoryBlock]
    val buffer = UcxUtils.getByteBufferView(mem.address, mem.size)

    def send(workerWrapper: UcxWorkerWrapper, currentId: Int): Unit = try {
      val remaining = blockSlice.length - currentId - 1
      val currentOffset = blockSlice(currentId)
      val currentSize = (blockSize - currentOffset).min(maxBodySize)
      buffer.clear()
      buffer.limit(headerSize + currentSize.toInt)
      buffer.putInt(replyTag)
      buffer.putInt(remaining)
      block.getBlock(buffer, currentOffset)

      val startTime = System.nanoTime()
      val ep = workerWrapper.connections(replyExecutor)
      workerWrapper.worker.synchronized {
        ep.sendAmNonBlocking(2, mem.address, headerSize,
          mem.address + headerSize, currentSize, 0, new UcxCallback {
            override def onSuccess(request: UcpRequest): Unit = {
              logTrace(s"Reply stream block $currentId size $currentSize tag $replyTag" +
                s" in ${System.nanoTime() - startTime} ns.")
              if (remaining > 0) {
                transport.replyThreadPool.submit(new Runnable {
                  override def run = send(
                    transport.selectServerWorker, currentId + 1)
                })
              } else {
                mem.close()
              }
            }
            override def onError(ucsStatus: Int, errorMsg: String): Unit = {
              logError(s"Failed to reply stream $errorMsg")
            }
          }, new UcpRequestParams()
            .setMemoryType(UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
            .setMemoryHandle(mem.memory))
      }
    } catch {
      case ex: Throwable => logError(s"Failed to reply stream $ex.")
    }
    send(this, 0)
  }

}

// class UcxWorkerThread(val workerWrapper: UcxWorkerWrapper) extends Thread with Logging {
//   val id = workerWrapper.id
//   val worker = workerWrapper.worker
//   val transport = workerWrapper.transport
//   val useWakeup = workerWrapper.transport.ucxShuffleConf.useWakeup

//   private val taskQueue = new ConcurrentLinkedQueue[Runnable]()

//   setDaemon(true)
//   setName(s"UCX-worker $id")

//   override def run(): Unit = {
//     logDebug(s"UCX-worker $id started")
//     while (!isInterrupted) {
//       Option(taskQueue.poll()) match {
//         case Some(task) => task.run
//         case None => {}
//       }
//       worker.synchronized {
//         while (worker.progress() != 0) {}
//       }
//       if(taskQueue.isEmpty && useWakeup) {
//         worker.waitForEvents()
//       }
//     }
//     logDebug(s"UCX-worker $id stopped")
//   }

//   @inline
//   def submit(task: Callable[_]): Future[_] = {
//     val future = new FutureTask(task)
//     taskQueue.offer(future)
//     worker.signal()
//     future
//   }

//   @inline
//   def submit(task: Runnable): Future[Unit.type] = {
//     val future = new FutureTask(task, Unit)
//     taskQueue.offer(future)
//     worker.signal()
//     future
//   }

//   @inline
//   def close(): Unit = {
//     interrupt()
//     join(10)
//     workerWrapper.close()
//   }
// }