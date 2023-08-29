/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import java.net.InetSocketAddress
import java.io.Closeable
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap
import scala.util.Random
import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants
import org.openucx.jucx.ucs.UcsConstants.MEMORY_TYPE
import org.openucx.jucx.{UcxCallback, UcxException, UcxUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.memory.UcxBounceBufferMemoryBlock
import org.apache.spark.shuffle.ucx.utils.{SerializationUtils, UcpSparkAmId}
import org.apache.spark.shuffle.utils.{UnsafeUtils, CommonUtils}
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.ThreadUtils

import java.nio.{ByteBuffer, ByteOrder}
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
case class UcxWorkerWrapper(worker: UcpWorker, transport: UcxShuffleTransport, id: Long = 0L)
  extends Closeable with Logging {

  private final val connections =  new TrieMap[transport.ExecutorId, UcpEndpoint]
  private final val dpuAddress =  new TrieMap[InetSocketAddress, UcpEndpoint]
  private val requestData = new TrieMap[Int, (Seq[OperationCallback], UcxRequest, transport.BufferAllocator)]
  private val tag = new AtomicInteger(Random.nextInt())
  private val flushRequests = new ConcurrentLinkedQueue[UcpRequest]()

  private val ioThreadPool = ThreadUtils.newForkJoinPool("IO threads",
    transport.ucxShuffleConf.numIoThreads)
  private val ioTaskSupport = new ForkJoinTaskSupport(ioThreadPool)

  private var connectToRemoteNvkv = false
  private var requestComplete = false

  val synMon = transport.synMon
  val synLat = transport.synLat
  val fetchMon = transport.fetchMon
  val fetchLat = transport.fetchLat
  val replyMon = transport.replyMon
  val replyLat = transport.replyLat
  val fetchReplyMon = transport.fetchReplyMon
  val fetchReplyLat = transport.fetchReplyLat

  worker.setAmRecvHandler(UcpSparkAmId.InitExecutorAck,
  (headerAddress: Long, headerSize: Long, ucpAmData: UcpAmData, _: UcpEndpoint) => {

    logDebug(s"LEO InitExecutorAck called!")

    if (ucpAmData.isDataValid) {
      // val buff: ByteBuffer = UcxUtils.getByteBufferView(ucpAmData.getDataAddress, ucpAmData.getLength.toInt)
      val buf: ByteBuffer = UcxUtils.getByteBufferView(ucpAmData.getDataAddress, ucpAmData.getLength.toInt)
      var bytes: Array[Byte] = new Array[Byte](ucpAmData.getLength.toInt);
      buf.get(bytes);
      transport.getNvkvWrapper.connectToRemote(bytes)
      connectToRemoteNvkv = true
    } else {
      logDebug(s"LEO InitExecutorAck handler Invalid data!!!")  
    }

    logDebug(s"LEO Connected to remote nvkv on DPU")
    UcsConstants.STATUS.UCS_OK
  }, UcpConstants.UCP_AM_FLAG_PERSISTENT_DATA | UcpConstants.UCP_AM_FLAG_WHOLE_MSG)

  worker.setAmRecvHandler(UcpSparkAmId.FetchBlockReqAck,
    (headerAddress: Long, headerSize: Long, ucpAmData: UcpAmData, _: UcpEndpoint) => {
      val headerBuffer = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
      // val headerBuffer = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt).order(ByteOrder.nativeOrder())
      val i = headerBuffer.getInt
      val data = requestData.remove(i)

      logInfo(s"@Z Recv tag $i length ${ucpAmData.getLength}")
      // logDebug(s"LEO Fetch block ack called!")

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
        // logDebug(s"Received amData: $ucpAmData for tag $i " +
        //   s"in ${stats.getElapsedTimeNs} ns")
        val elapsedTime = stats.getElapsedTimeNs/1000
        fetchReplyMon.update(IntArrayRecord.add(_, (Interval.toLog10(elapsedTime), 1)))
        fetchReplyLat.update(LongRecord.add(_, elapsedTime))

        for (b <- 0 until numBlocks) {
          val blockSize = headerBuffer.getInt
          if (callbacks(b) != null) {
            callbacks(b).onComplete(new OperationResult {
              override def getStatus: OperationStatus.Value = OperationStatus.SUCCESS

              override def getError: TransportError = null

              override def getStats: Option[OperationStats] = Some(stats)

              override def getData: MemoryBlock = new UcxAmDataMemoryBlock(ucpAmData, offset, blockSize, refCounts)
            })
          }
          offset += alignedLength(blockSize, 512)
        }
        if (callbacks.isEmpty) UcsConstants.STATUS.UCS_OK else UcsConstants.STATUS.UCS_INPROGRESS

      } else {
        logDebug(s"LEO Received RNDV rts Length: ${stats.receiveSize}")
        val mem = allocator(ucpAmData.getLength)
        stats.amHandleTime = System.nanoTime()
        request.setRequest(worker.recvAmDataNonBlocking(ucpAmData.getDataHandle, mem.address, ucpAmData.getLength,
            new UcxCallback() {
              override def onSuccess(r: UcpRequest): Unit = {
                request.completed = true
                stats.endTime = System.nanoTime()
                // logDebug(s"Received rndv data of size: ${mem.size} for tag $i in " +
                //   s"${stats.getElapsedTimeNs} ns " +
                //   s"time from amHandle: ${System.nanoTime() - stats.amHandleTime} ns")
                val elapsedTime = stats.getElapsedTimeNs/1000
                fetchReplyMon.update(IntArrayRecord.add(_, (Interval.toLog10(elapsedTime), 1)))
                fetchReplyLat.update(LongRecord.add(_, elapsedTime))
                for (b <- 0 until numBlocks) {
                  val blockSize = headerBuffer.getInt
                  if (callbacks(b) != null) {
                    callbacks(b).onComplete(new OperationResult {
                      override def getStatus: OperationStatus.Value = OperationStatus.SUCCESS

                      override def getError: TransportError = null

                      override def getStats: Option[OperationStats] = Some(stats)

                      override def getData: MemoryBlock = new UcxRefCountMemoryBlock(mem, offset, blockSize, refCounts)
                    })
                  }
                  offset += alignedLength(blockSize, 512)
                }
              }
            }, UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST))
        UcsConstants.STATUS.UCS_OK
      }
    }, UcpConstants.UCP_AM_FLAG_PERSISTENT_DATA | UcpConstants.UCP_AM_FLAG_WHOLE_MSG)

  @inline
  def alignedLength(number: Int, alignment: Int) =
    (((number) + (alignment)-1) & ~((alignment)-1))

  override def close(): Unit = {
    val closeRequests = connections.map {
      case (_, endpoint) => endpoint.closeNonBlockingForce()
    }

    CommonUtils.safePolling(() => {progress()},
      () => {!closeRequests.forall(_.isCompleted)})
    ioThreadPool.shutdown()
    connections.clear()
    worker.close()
  }

  /**
   * Blocking progress until there's outstanding flush requests.
   */
  def progressConnect(): Unit = {
    CommonUtils.safePolling(() => {
        progress()
        flushRequests.removeIf(_.isCompleted)
      },() => {!flushRequests.isEmpty})
    logTrace(s"Flush completed. Number of connections: ${connections.keys.size}")
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
    val ep = worker.newEndpoint(new UcpEndpointParams().setName(s"Server connection to $executorId")
      .setUcpAddress(workerAddress))
    connections.put(executorId, ep)
  }

  def getConnection(executorId: transport.ExecutorId): UcpEndpoint = {
    // TODO: Skip connection if already connected to the DPU of this executor

    CommonUtils.safePolling(() => {},
      () => {!transport.executorAddresses.contains(executorId)}, 
      transport.ucxShuffleConf.getSparkConf.getTimeAsMs("spark.network.timeout", "100"),
      s"RPC timeout - Waiting for DPU address for $executorId")

    connections.getOrElseUpdate(executorId,  {
      val address = transport.executorAddresses(executorId)
      val deserializedAddress = SerializationUtils.deserializeInetAddress(address)

      dpuAddress.getOrElseUpdate(deserializedAddress, {
        val endpointParams = new UcpEndpointParams().setPeerErrorHandlingMode()
          .setSocketAddress(deserializedAddress).sendClientId()
          .setErrorHandler(new UcpEndpointErrorHandler() {
            override def onError(ep: UcpEndpoint, status: Int, errorMsg: String): Unit = {
              logError(s"Endpoint to $executorId got an error: $errorMsg")
              dpuAddress.remove(deserializedAddress)
            }
          }).setName(s"Endpoint to DPU/$deserializedAddress")

        logDebug(s"Worker $this connecting to DPU($deserializedAddress)")
        val ep = worker.newEndpoint(endpointParams)
        ep
      })
    })
  }

  def fetchBlocksByBlockIds(executorId: transport.ExecutorId, blockIds: Seq[BlockId],
                            resultBufferAllocator: transport.BufferAllocator,
                            callbacks: Seq[OperationCallback],
                            amRecvStartCb: () => Unit): Seq[Request] = {
    val startTime = System.nanoTime()
    val headerSize = UnsafeUtils.INT_SIZE * 2
    val ep = getConnection(executorId)

    val t = tag.incrementAndGet()

    val buffer = Platform.allocateDirectBuffer(headerSize + blockIds.map(_.serializedSize).sum)
    // val buffer = Platform.allocateDirectBuffer(headerSize + blockIds.map(_.serializedSize).sum).order(ByteOrder.nativeOrder())
    buffer.putInt(t)
    buffer.putInt(blockIds.size)
    blockIds.foreach(b => b.serialize(buffer))

    val request = new UcxRequest(null, new UcxStats())
    requestData.put(t, (callbacks, request, resultBufferAllocator))

    buffer.rewind()
    val address = UnsafeUtils.getAdress(buffer)
    val dataAddress = address + headerSize

    logInfo(s"@Z Send tag ${t}")
    // logDebug(s"Perftest send tag ${t} time ${System.nanoTime()}")
    ep.sendAmNonBlocking(UcpSparkAmId.FetchBlockReq, address, headerSize, dataAddress, buffer.capacity() - headerSize,
    UcpConstants.UCP_AM_SEND_FLAG_EAGER | UcpConstants.UCP_AM_SEND_FLAG_REPLY, new UcxCallback() {
      override def onSuccess(request: UcpRequest): Unit = {
        buffer.clear()
         logDebug(s"Sent message on $ep to $executorId to fetch ${blockIds.length} blocks on tag $t" +
           s"in ${System.nanoTime() - startTime} ns")
      }
    }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)

    progress()
    Seq(request)
  }

  def sendNvkvCtxToDpu(executorId: transport.ExecutorId, nvkvWrapper: NvkvWrapper,
                callback: OperationCallback): Request = {
    val startTime = System.nanoTime()
    val ep = getConnection(executorId)
    val t = tag.incrementAndGet()
    val length = nvkvWrapper.pack.capacity()

    val buffer = Platform.allocateDirectBuffer(length)
    buffer.put(nvkvWrapper.pack)
    buffer.rewind()


    val request = new UcxRequest(null, new UcxStats())

    val address = UnsafeUtils.getAdress(buffer)
    logDebug(s"Sending message to init executer $executorId with length $length")

    ep.sendAmNonBlocking(UcpSparkAmId.InitExecutorReq, 0, 0, address, length,
      UcpConstants.UCP_AM_SEND_FLAG_EAGER | UcpConstants.UCP_AM_SEND_FLAG_REPLY, new UcxCallback() {
       override def onSuccess(request: UcpRequest): Unit = {
         buffer.clear()
         logDebug(s"Sent message on $ep to $executorId to ini executer" +
           s"in ${System.nanoTime() - startTime}ns")
       }

       override def onError(ucsStatus: Int, errorMsg: String): Unit = {
          logError(s"Failed to send init executer message $errorMsg")
        }
     }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)

    CommonUtils.safePolling(() => {progress()},
      () => {!connectToRemoteNvkv}, 
      10*1000,
      s"Connect to remote $address failed")
    request
  }

    def commitBlock(executorId: transport.ExecutorId, nvkvWrapper: NvkvWrapper,
                resultBufferAllocator: transport.BufferAllocator, packMapperData: ByteBuffer,
                sendCompleteCB: () => Unit): Request = {
    val startTime = System.nanoTime()
    val ep = getConnection(executorId)
    val t = tag.incrementAndGet()
    val length = packMapperData.capacity()

    val buffer = Platform.allocateDirectBuffer(length)
    buffer.put(packMapperData)
    buffer.rewind()


    val request = new UcxRequest(null, new UcxStats())

    val address = UnsafeUtils.getAdress(buffer)
    logDebug(s"Sending message to commit mapper info with length $length")

    ep.sendAmNonBlocking(UcpSparkAmId.MapperInfo, 0, 0, address, length,
      UcpConstants.UCP_AM_SEND_FLAG_EAGER | UcpConstants.UCP_AM_SEND_FLAG_REPLY, new UcxCallback() {
       override def onSuccess(request: UcpRequest): Unit = {
         buffer.clear()
         sendCompleteCB()
         logDebug(s"Sent message on $ep to $executorId to ini executer" +
           s"in ${System.nanoTime() - startTime}ns")
       }

       override def onError(ucsStatus: Int, errorMsg: String): Unit = {
          logError(s"Failed to send init executer message $errorMsg")
        }
     }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)

    progress()
    request
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
    val blocksCollection = if (transport.ucxShuffleConf.numIoThreads > 1) {
      val parCollection = blocks.indices.par
      parCollection.tasksupport = ioTaskSupport
      parCollection
    } else {
      blocks.indices
    }

    for (i <- blocksCollection) {
      blocks(i).getBlock(localBuffers(i))
    }

    val startTime = System.nanoTime()
    val req = getConnection(replyExecutor).sendAmNonBlocking(0 , resultMemory.address, tagAndSizes,
      resultMemory.address + tagAndSizes, resultMemory.size - tagAndSizes, 0, new UcxCallback {
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

    CommonUtils.safePolling(() => {progress()},
      () => {!req.isCompleted}, 10*1000,
      s"Failed when sending fetch block request")
  } catch {
    case ex: Throwable => logError(s"Failed to read and send data: $ex")
  }

}
