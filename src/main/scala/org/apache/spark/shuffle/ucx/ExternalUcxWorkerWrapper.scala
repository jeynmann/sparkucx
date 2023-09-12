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
import java.net.InetSocketAddress

/**
 * Worker per thread wrapper, that maintains connection and progress logic.
 */
case class ExternalUcxWorkerWrapper(worker: UcpWorker,
                                    transport: ExternalShuffleTransport,
                                    isClientWorker: Boolean, workerId: UcxWorkerId)
  extends Closeable with Logging {
  private lazy val shuffleServers = new TrieMap[InetSocketAddress, UcpEndpoint]
  private lazy val shuffleClients = new TrieMap[UcxWorkerId, UcpEndpoint]
  private lazy val requestData = new TrieMap[Int, (Seq[OperationCallback], UcxRequest)]
  private lazy val tag = new AtomicInteger(Random.nextInt())
  private[this] val flushRequests = new ConcurrentLinkedQueue[UcpRequest]()
  private[this] val memPool = transport.hostBounceBufferMemoryPool

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

        val (callbacks, request) = data.get
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
    val closeRequests = shuffleServers.map {
      case (_, endpoint) => endpoint.closeNonBlockingForce()
    }
    while (!closeRequests.forall(_.isCompleted)) {
      progress()
    }
    shuffleServers.clear()
    worker.close()
  }

  /**
   * Blocking progress until there's outstanding flush requests.
   */
  def progressConnect(): Unit = {
    while (!flushRequests.isEmpty) {
      progress()
      flushRequests.removeIf(_.isCompleted)
    }
    logTrace(s"Flush completed. Number of shuffleServers: ${shuffleServers.keys.size}")
  }

  /**
   * The only place for worker progress
   */
  def progress(): Int = worker.synchronized {
    worker.progress()
  }

  def getConnection(shuffleServer: InetSocketAddress): UcpEndpoint = {
    shuffleServers(shuffleServer)
  }

  def getConnectionBack(shuffleClient: UcxWorkerId): UcpEndpoint = {
    shuffleClients(shuffleClient)
  }

  def connectBack(shuffleClient: UcxWorkerId, workerAddress: ByteBuffer): Unit = {
    logDebug(s"Worker $this connecting back to $UcxWorkerId by worker address")
    val ep = worker.newEndpoint(new UcpEndpointParams().setName(s"Server connection to $UcxWorkerId")
      .setUcpAddress(workerAddress))
    shuffleClients.put(shuffleClient, ep)
  }

  def connect(shuffleServer: InetSocketAddress, workerAddress: ByteBuffer): Unit = {
    shuffleServers.getOrElseUpdate(shuffleServer, {
      val endpointParams = new UcpEndpointParams().setPeerErrorHandlingMode()
        .setSocketAddress(shuffleServer).sendClientId()
        .setErrorHandler(new UcpEndpointErrorHandler() {
          override def onError(ep: UcpEndpoint, status: Int, errorMsg: String): Unit = {
            logError(s"Endpoint to $shuffleServer got an error: $errorMsg")
            shuffleServers.remove(shuffleServer)
          }
        }).setName(s"Endpoint to $shuffleServer")

      logDebug(s"Worker $this connecting to ExternalShuffleService($shuffleServer)")

      val header = Platform.allocateDirectBuffer(workerId.serializedSize)
      workerId.serialize(header)
      header.rewind()
      val workerAddress = worker.getAddress

      val ep = worker.newEndpoint(endpointParams)
      ep.sendAmNonBlocking(1, UcxUtils.getAddress(header), UnsafeUtils.LONG_SIZE,
        UcxUtils.getAddress(workerAddress), workerAddress.capacity().toLong, UcpConstants.UCP_AM_SEND_FLAG_EAGER,
        new UcxCallback() {
          override def onSuccess(request: UcpRequest): Unit = {
            header.clear()
            workerAddress.clear()
          }
        }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
      flushRequests.add(ep.flushNonBlocking(null))
      ep
    })
  }

  def fetchBlocksByBlockIds(shuffleServer: InetSocketAddress, blockIds: Seq[BlockId],
                            callbacks: Seq[OperationCallback]): Unit = {
    val startTime = System.nanoTime()
    val headerSize = workerId.serializedSize + UnsafeUtils.INT_SIZE

    val t = tag.incrementAndGet()

    val buffer = Platform.allocateDirectBuffer(headerSize + blockIds.map(_.serializedSize).sum)
    workerId.serialize(buffer)
    buffer.putInt(t)
    blockIds.foreach(b => b.serialize(buffer))

    val request = new UcxRequest(null, new UcxStats())
    requestData.put(t, (callbacks, request))

    buffer.rewind()
    val address = UnsafeUtils.getAdress(buffer)
    val dataAddress = address + headerSize

    val ep = getConnection(shuffleServer)
    ep.sendAmNonBlocking(0, address,
      headerSize, dataAddress, buffer.capacity() - headerSize,
      UcpConstants.UCP_AM_SEND_FLAG_EAGER, new UcxCallback() {
       override def onSuccess(request: UcpRequest): Unit = {
         buffer.clear()
         logDebug(s"Sent message on $ep to $shuffleServer to fetch ${blockIds.length} blocks on tag $t $workerId" +
           s"in ${System.nanoTime() - startTime} ns")
       }
     }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
  }

  def handleFetchBlockRequest(clientWorker: UcxWorkerId, replyTag: Int, blocks: Seq[Block]): Unit = try {
    val tagAndSizes = UnsafeUtils.INT_SIZE + UnsafeUtils.INT_SIZE * blocks.size
    val resultMemory = memPool.get(tagAndSizes + blocks.map(_.getSize).sum)
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

    for (i <- blocks.indices) {
      blocks(i).getBlock(localBuffers(i))
    }

    val startTime = System.nanoTime()
    getConnectionBack(clientWorker).sendAmNonBlocking(1, resultMemory.address, tagAndSizes,
      resultMemory.address + tagAndSizes, resultMemory.size - tagAndSizes, 0, new UcxCallback {
        override def onSuccess(request: UcpRequest): Unit = {
          logTrace(s"Sent ${blocks.length} blocks of size: ${resultMemory.size} " +
            s"to tag $replyTag in ${System.nanoTime() - startTime} ns.")
          memPool.put(resultMemory)
        }

        override def onError(ucsStatus: Int, errorMsg: String): Unit = {
          logError(s"Failed to send $errorMsg")
        }
      }, new UcpRequestParams().setMemoryType(UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
        .setMemoryHandle(resultMemory.memory))
  } catch {
    case ex: Throwable => logError(s"Failed to read and send data: $ex")
  }
}

class ExternalUcxWorkerThread(
  worker: UcpWorker, transport: ExternalShuffleTransport,
  isClientWorker: Boolean, workerId: UcxWorkerId = new UcxWorkerId(0, 0, 0))
  extends Thread with Logging {
  val workerWrapper = ExternalUcxWorkerWrapper(worker, transport, isClientWorker, workerId)

  private[this] val useWakeup = transport.ucxShuffleConf.useWakeup
  private[this] val taskQueue = new ConcurrentLinkedQueue[Runnable]()

  setDaemon(true)
  setName(s"UCX-worker $workerId")

  override def run(): Unit = {
    logDebug(s"UCX-worker $workerId started")
    if (useWakeup) {
      while (!isInterrupted) {
        Option(taskQueue.poll()) match {
          case Some(task) => task.run
          case None => {}
        }
        while (worker.progress() != 0) {}
        if(taskQueue.isEmpty) {
          worker.waitForEvents()
        }
      }
    } else {
      while (!isInterrupted) {
        Option(taskQueue.poll()) match {
          case Some(task) => task.run
          case None => {}
        }
        worker.progress()
      }
    }
    logDebug(s"UCX-worker $workerId stopped")
  }

  @inline
  def submit(task: Callable[_]): Future[_] = {
    val future = new FutureTask(task)
    taskQueue.offer(future)
    worker.signal()
    future
  }

  @inline
  def submit(task: Runnable): Future[Unit.type] = {
    val future = new FutureTask(task, Unit)
    taskQueue.offer(future)
    worker.signal()
    future
  }

  @inline
  def close(): Unit = {
    interrupt()
    join(10)
    workerWrapper.close()
  }
}