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

/**
 * Worker per thread wrapper, that maintains connection and progress logic.
 */
case class UcxWorkerWrapper(worker: UcpWorker, transport: UcxShuffleTransport, isClientWorker: Boolean,
                            id: Long = 0L)
  extends Closeable with Logging {
  private[ucx] final val timeout = transport.ucxShuffleConf.getSparkConf.getTimeAsMs("spark.network.timeout", "100")
  private[ucx] final val connections =  new TrieMap[transport.ExecutorId, UcpEndpoint]
  private[ucx] lazy val requestData = new TrieMap[Int, (Seq[OperationCallback], UcxRequest, transport.ExecutorId)]
  private[ucx] lazy val tag = new AtomicInteger(Random.nextInt())
  private[ucx] lazy val memPool = transport.hostBounceBufferMemoryPool

  private[ucx] lazy val ioThreadOn = transport.ucxShuffleConf.numIoThreads > 1
  private[ucx] lazy val ioThreadPool = ThreadUtils.newForkJoinPool("IO threads",
    transport.ucxShuffleConf.numIoThreads)
  private[ucx] lazy val ioTaskSupport = new ForkJoinTaskSupport(ioThreadPool)

  private[this] val pollSend = transport.pollSend
  private[this] val pollRecv = transport.pollRecv
  private[this] val flySend = transport.flySend
  private[this] val flyRecv = transport.flyRecv
  private[this] val synSend = transport.synSend
  private[this] val synRecv = transport.synRecv
  // private[this] val txBps = transport.txBps
  // private[this] val rxBps = transport.rxBps

  if (isClientWorker) {
    // Receive block data handler
    worker.setAmRecvHandler(1,
      (headerAddress: Long, headerSize: Long, ucpAmData: UcpAmData, _: UcpEndpoint) => {
        val headerBuffer = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
        val i = headerBuffer.getInt
        val data = requestData.remove(i)

        if (data.isEmpty) {
          throw new UcxException(s"No data for tag $i.")
        }

        val (callbacks, request, execId) = data.get
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
          val mem = memPool.get(ucpAmData.getLength)
          // stats.amHandleTime = System.nanoTime()
          request.setRequest(worker.recvAmDataNonBlocking(ucpAmData.getDataHandle, mem.address, ucpAmData.getLength,
            new UcxCallback() {
              override def onSuccess(r: UcpRequest): Unit = {
                request.completed = true
                // stats.endTime = System.nanoTime()
                // logDebug(s"Received rndv data of size: ${mem.size} for tag $i in " +
                //   s"${stats.getElapsedTimeNs} ns " +
                //   s"time from amHandle: ${System.nanoTime() - stats.amHandleTime} ns")
                val recvTime = UcxUtils.getByteBufferView(mem.address, ucpAmData.getLength).getLong
                offset += UnsafeUtils.LONG_SIZE
                flyRecv(execId.toInt).add(System.currentTimeMillis - recvTime)
                // rxBps.add(headerBuffer.capacity + mem.size)
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
    if (ioThreadOn) {
      ioThreadPool.shutdown()
    }
    worker.close()
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
      flySend.getOrElseUpdate(executorId.toInt, new PsMonitor)
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
        flyRecv.getOrElseUpdate(executorId.toInt, new PsMonitor)
        ep.sendAmNonBlocking(1, UcxUtils.getAddress(header), UnsafeUtils.LONG_SIZE,
          UcxUtils.getAddress(workerAddress), workerAddress.capacity().toLong, UcpConstants.UCP_AM_SEND_FLAG_EAGER,
          new UcxCallback() {
            override def onSuccess(request: UcpRequest): Unit = {
              header.clear()
              workerAddress.clear()
            }
          }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
        ep
      }
    })
  }

  def fetchBlocksByBlockIds(executorId: transport.ExecutorId, blockIds: Seq[BlockId],
                            resultBufferAllocator: transport.BufferAllocator,
                            callbacks: Seq[OperationCallback]): Unit = {
    val headerSize = UnsafeUtils.INT_SIZE + UnsafeUtils.LONG_SIZE
    
    val t = tag.incrementAndGet()

    val buffer = Platform.allocateDirectBuffer(headerSize + UnsafeUtils.LONG_SIZE + blockIds.map(_.serializedSize).sum)
    buffer.putInt(t)
    buffer.putLong(id)
    buffer.putLong(0L)
    blockIds.foreach(b => b.serialize(buffer))

    val request = new UcxRequest(null, new UcxStats())
    requestData.put(t, (callbacks, request, executorId))

    val startTime = System.currentTimeMillis
    buffer.putLong(headerSize, startTime)
    buffer.clear()
    val address = UnsafeUtils.getAdress(buffer)
    val dataAddress = address + headerSize

    val ep = getConnection(executorId)
    worker.synchronized {
      synSend.add(System.currentTimeMillis - startTime)
      ep.sendAmNonBlocking(0, address,
        headerSize, dataAddress, buffer.capacity() - headerSize,
        UcpConstants.UCP_AM_SEND_FLAG_EAGER, new UcxCallback() {
        override def onSuccess(request: UcpRequest): Unit = {
          buffer.clear()
          pollSend.add(System.currentTimeMillis - startTime)
        }
      }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
    }
  }

  def handleFetchBlockRequest(blocks: Seq[Block], replyTag: Int, replyExecutor: Long): Unit = try {
    val tagAndSizes = UnsafeUtils.INT_SIZE + UnsafeUtils.INT_SIZE * blocks.length
    val tagAndStamp = tagAndSizes + UnsafeUtils.LONG_SIZE
    val resultMemory = transport.hostBounceBufferMemoryPool.get(tagAndStamp + blocks.map(_.getSize).sum)
      .asInstanceOf[UcxBounceBufferMemoryBlock]
    val resultBuffer = UcxUtils.getByteBufferView(resultMemory.address,
      resultMemory.size)
    resultBuffer.putInt(replyTag)

    var offset = 0
    val localBuffers = blocks.zipWithIndex.map {
      case (block, i) =>
        resultBuffer.putInt(UnsafeUtils.INT_SIZE + i * UnsafeUtils.INT_SIZE, block.getSize.toInt)
        resultBuffer.position(tagAndStamp + offset)
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
      blocks(i).getBlock(localBuffers(i))
    }

    val startTime = System.currentTimeMillis()
    resultBuffer.putLong(tagAndSizes, startTime)
    val ep = connections(replyExecutor)
    worker.synchronized {
      synRecv.add(System.currentTimeMillis - startTime)
      ep.sendAmNonBlocking(1, resultMemory.address, tagAndSizes,
        resultMemory.address + tagAndSizes, resultMemory.size - tagAndSizes,
        0, new UcxCallback {
          override def onSuccess(request: UcpRequest): Unit = {
            transport.hostBounceBufferMemoryPool.put(resultMemory)
            // logTrace(s"Sent ${blocks.length} blocks of size: ${resultMemory.size} " +
            //   s"to tag $replyTag in ${System.nanoTime() - startTime} ns.")
            pollRecv.add(System.currentTimeMillis - startTime)
            // txBps.add(resultMemory.size)
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