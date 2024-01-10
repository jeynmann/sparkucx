/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import java.io.Closeable
import java.nio.channels.ReadableByteChannel
import java.util.concurrent.{ConcurrentLinkedQueue, Callable, Future, FutureTask}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap
import scala.util.Random
import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants
import org.openucx.jucx.ucs.UcsConstants.MEMORY_TYPE
import org.openucx.jucx.{UcxCallback, UcxException, UcxUtils}
import org.apache.spark.shuffle.ucx.memory.UcxBounceBufferMemoryBlock
import org.apache.spark.shuffle.ucx.utils.SerializationUtils
import org.apache.spark.shuffle.utils.{UnsafeUtils, UcxLogging}
import org.apache.spark.unsafe.Platform

import java.nio.ByteBuffer
import java.net.InetSocketAddress

/**
 * Worker per thread wrapper, that maintains connection and progress logic.
 */
case class ExternalUcxClientWorker(val worker: UcpWorker,
                                   transport: ExternalUcxClientTransport,
                                   workerId: UcxWorkerId)
  extends Closeable with UcxLogging {
  private[this] lazy val shuffleServers = new TrieMap[InetSocketAddress, UcpEndpoint]
  private[this] lazy val requestData = new TrieMap[Int, (Seq[OperationCallback], UcxRequest)]
  private[this] lazy val streamData = new TrieMap[Int, UcxStreamState]
  private[this] lazy val sliceData = new TrieMap[Int, UcxSliceState]
  private[this] lazy val tag = new AtomicInteger(Random.nextInt())
  private[this] lazy val memPool = transport.hostBounceBufferMemoryPool
  private[this] lazy val maxReplySize = transport.ucxShuffleConf.maxReplySize

  private[this] case class UcxAddressReplyHandle() extends UcpAmRecvCallback() {
    override def onReceive(headerAddress: Long, headerSize: Long,
                           ucpAmData: UcpAmData, ep: UcpEndpoint): Int = {
      transport.serverMap.getOrElseUpdate(transport.tcpServer, {
        val msg = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
        val ucpAddress = ByteBuffer.allocateDirect(msg.remaining())
        ucpAddress.put(msg)
        ucpAddress
      })
      UcsConstants.STATUS.UCS_OK
    }
  }

  private[this] case class UcxSliceReplyHandle() extends UcpAmRecvCallback() {
    override def onReceive(headerAddress: Long, headerSize: Long,
                           ucpAmData: UcpAmData, ep: UcpEndpoint): Int = {
      val headerBuffer = UnsafeUtils.getByteBufferView(headerAddress,
                                                       headerSize.toInt)
      val i = headerBuffer.getInt
      val remaining = headerBuffer.getInt

      val sliceState = sliceData.getOrElseUpdate(i, {
        requestData.remove(i) match {
          case Some(data) => {
            val mem = memPool.get(maxReplySize * (remaining + 1))
            new UcxSliceState(data._1(0), data._2, mem, 0L, Int.MaxValue)
          }
          case None => throw new UcxException(s"Slice tag $i context not found.")
        }
      })

      if (remaining >= sliceState.remaining) {
        throw new UcxException(
          s"Slice tag $i out of order $remaining <= ${sliceState.remaining}.")
      }
      sliceState.remaining = remaining

      val stats = sliceState.request.getStats.get.asInstanceOf[UcxStats]
      stats.receiveSize += ucpAmData.getLength

      val currentAddress = sliceState.mem.address + sliceState.offset
      if (ucpAmData.isDataValid) {
        stats.endTime = System.nanoTime()
        logDebug(s"Slice receive amData ${ucpAmData} tag $i in "
          + s"${stats.getElapsedTimeNs} ns")
        val curBuf = UnsafeUtils.getByteBufferView(
          ucpAmData.getDataAddress, ucpAmData.getLength.toInt)
        val buffer = UnsafeUtils.getByteBufferView(
          currentAddress, ucpAmData.getLength.toInt)
        buffer.put(curBuf)
        sliceState.offset += ucpAmData.getLength()
        if (remaining == 0) {
          val result = new UcxRefCountMemoryBlock(sliceState.mem, 0,
                                                  sliceState.offset,
                                                  new AtomicInteger(1))
          sliceState.callback.onComplete(
            new UcxSucceedOperationResult(result, stats))
          sliceData.remove(i)
        }
      } else {
        stats.amHandleTime = System.nanoTime()
        worker.recvAmDataNonBlocking(
          ucpAmData.getDataHandle, currentAddress, ucpAmData.getLength,
          new UcxCallback() {
            override def onSuccess(r: UcpRequest): Unit = {
              stats.endTime = System.nanoTime()
              logDebug(s"Slice receive rndv data size ${ucpAmData.getLength} " +
                s"tag $i in ${stats.getElapsedTimeNs} ns amHandle " +
                s"${stats.endTime - stats.amHandleTime} ns")
              sliceState.offset += ucpAmData.getLength()
              if (remaining == 0) {
                val result = new UcxRefCountMemoryBlock(sliceState.mem, 0,
                                                        sliceState.offset,
                                                        new AtomicInteger(1))
                sliceState.callback.onComplete(
                  new UcxSucceedOperationResult(result, stats))
                sliceData.remove(i)
              }
            }
          }, UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
      }
      UcsConstants.STATUS.UCS_OK
    }
  }

  private[this] case class UcxStreamReplyHandle() extends UcpAmRecvCallback() {
    override def onReceive(headerAddress: Long, headerSize: Long,
                           ucpAmData: UcpAmData, ep: UcpEndpoint): Int = {
      val headerBuffer = UnsafeUtils.getByteBufferView(headerAddress,
                                                       headerSize.toInt)
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

      if (ucpAmData.isDataValid) {
        stats.endTime = System.nanoTime()
        logDebug(s"Stream receive amData ${ucpAmData} tag $i in "
          + s"${stats.getElapsedTimeNs} ns")
        val buffer = UnsafeUtils.getByteBufferView(
          ucpAmData.getDataAddress, ucpAmData.getLength.toInt)
        streamState.callback.onData(buffer)
        if (remaining == 0) {
          streamState.callback.onComplete(
            new UcxSucceedOperationResult(null, stats))
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
              logDebug(s"Stream receive rndv data ${ucpAmData.getLength} " +
                s"tag $i in ${stats.getElapsedTimeNs} ns amHandle " +
                s"${stats.endTime - stats.amHandleTime} ns")
              val buffer = UnsafeUtils.getByteBufferView(
                mem.address, ucpAmData.getLength.toInt)
              streamState.callback.onData(buffer)
              mem.close()
              if (remaining == 0) {
                streamState.callback.onComplete(
                  new UcxSucceedOperationResult(null, stats))
                streamData.remove(i)
              }
            }
          }, UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
      }
      UcsConstants.STATUS.UCS_OK
    }
  }

  // Receive block data handler
  worker.setAmRecvHandler(ExternalUcxAmId.REPLY_ADDRESS, UcxAddressReplyHandle(),
                          UcpConstants.UCP_AM_FLAG_WHOLE_MSG)
  worker.setAmRecvHandler(ExternalUcxAmId.REPLY_SLICE, UcxSliceReplyHandle(),
                          UcpConstants.UCP_AM_FLAG_WHOLE_MSG)
  worker.setAmRecvHandler(ExternalUcxAmId.REPLY_STREAM, UcxStreamReplyHandle(),
                          UcpConstants.UCP_AM_FLAG_WHOLE_MSG)
  worker.setAmRecvHandler(ExternalUcxAmId.REPLY_BLOCK,
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
            callbacks(b).onComplete(new UcxSucceedOperationResult(
              new UcxAmDataMemoryBlock(ucpAmData, offset, blockSize,
                                        refCounts), stats))
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
              logDebug(s"Received rndv data of size: ${ucpAmData.getLength} " +
                s"for tag $i in ${stats.getElapsedTimeNs} ns " +
                s"time from amHandle: ${System.nanoTime() - stats.amHandleTime} ns")
              for (b <- 0 until numBlocks) {
                val blockSize = headerBuffer.getInt
                callbacks(b).onComplete(new UcxSucceedOperationResult(
                  new UcxRefCountMemoryBlock(mem, offset, blockSize,
                                              refCounts), stats))
                offset += blockSize
              }

            }
          }, UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST))
        UcsConstants.STATUS.UCS_OK
      }
    }, UcpConstants.UCP_AM_FLAG_PERSISTENT_DATA | UcpConstants.UCP_AM_FLAG_WHOLE_MSG)

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
   * The only place for worker progress
   */
  def progress(): Int = worker.synchronized {
    worker.progress()
  }

  def queryService(tcpServer: InetSocketAddress,
                   ucxServer: InetSocketAddress): Unit = {
    shuffleServers.getOrElseUpdate(tcpServer, {
      val endpointParams = new UcpEndpointParams().setPeerErrorHandlingMode()
        .setSocketAddress(ucxServer).sendClientId()
        .setErrorHandler(new UcpEndpointErrorHandler() {
          override def onError(ep: UcpEndpoint, status: Int, errorMsg: String): Unit = {
            logError(s"Endpoint to $ucxServer got an error: $errorMsg")
            shuffleServers.remove(tcpServer)
          }
        }).setName(s"Client to $ucxServer")

      val ep = worker.synchronized {
        worker.newEndpoint(endpointParams)
      }
      connect(ep, ExternalUcxAmId.ADDRESS, UcpConstants.UCP_AM_SEND_FLAG_REPLY)
      ep
    })
  }

  def getConnection(shuffleServer: InetSocketAddress): UcpEndpoint = {
    shuffleServers.getOrElseUpdate(shuffleServer, {
      val serverMap = transport.serverMap
      if (!serverMap.contains(shuffleServer)) {
        val timeout = transport.timeout
        val startTime = System.currentTimeMillis()
        while (!serverMap.contains(shuffleServer)) {
          if  (System.currentTimeMillis() - startTime > timeout) {
            throw new UcxException(s"Don't get a worker address for $UcxWorkerId")
          }
          Thread.`yield`
        }
      }
      val ucpAddress = serverMap(shuffleServer)
      val endpointParams = new UcpEndpointParams().setPeerErrorHandlingMode()
        .setUcpAddress(ucpAddress).sendClientId()
        .setErrorHandler(new UcpEndpointErrorHandler() {
          override def onError(ep: UcpEndpoint, status: Int, errorMsg: String): Unit = {
            logError(s"Endpoint to $shuffleServer got an error: $errorMsg")
            shuffleServers.remove(shuffleServer)
          }
        }).setName(s"Client to $shuffleServer")
      val ep = worker.synchronized {
        worker.newEndpoint(endpointParams)
      }
      connect(ep, ExternalUcxAmId.CONNECT, 0)
      ep
    })
  }

  private[this] def connect(ep: UcpEndpoint, amId: Int, flags: Long): Unit = {
    val header = Platform.allocateDirectBuffer(workerId.serializedSize)
    val workerAddress = worker.getAddress
    workerId.serialize(header)
    header.rewind()

    logInfo(s"$workerId connecting to external service $ep")

    worker.synchronized {
      ep.sendAmNonBlocking(amId, UcxUtils.getAddress(header),
                           header.remaining(),
                           UcxUtils.getAddress(workerAddress),
                           workerAddress.remaining(),
                           UcpConstants.UCP_AM_SEND_FLAG_EAGER | flags,
                           new UcxCallback() {
        override def onSuccess(request: UcpRequest): Unit = {
          header.clear()
          workerAddress.clear()
        }
        override def onError(ucsStatus: Int, errorMsg: String): Unit = {}
      }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
    }
  }

  def fetchBlocksByBlockIds(shuffleServer: InetSocketAddress, execId: Int,
                            blockIds: Seq[BlockId],
                            callbacks: Seq[OperationCallback]): Unit = {
    val startTime = System.nanoTime()
    val headerSize = UnsafeUtils.INT_SIZE + UnsafeUtils.INT_SIZE + workerId.serializedSize

    val t = tag.incrementAndGet()

    val buffer = Platform.allocateDirectBuffer(headerSize + blockIds.map(_.serializedSize).sum)
    workerId.serialize(buffer)
    buffer.putInt(t)
    buffer.putInt(execId)
    blockIds.foreach(b => b.serialize(buffer))

    val request = new UcxRequest(null, new UcxStats())
    requestData.put(t, (callbacks, request))

    buffer.rewind()
    val address = UnsafeUtils.getAdress(buffer)
    val dataAddress = address + headerSize

    val ep = getConnection(shuffleServer)
    worker.synchronized {
      ep.sendAmNonBlocking(ExternalUcxAmId.FETCH_BLOCK, address,
      headerSize, dataAddress, buffer.capacity() - headerSize,
      UcpConstants.UCP_AM_SEND_FLAG_EAGER, new UcxCallback() {
        override def onSuccess(request: UcpRequest): Unit = {
          buffer.clear()
          logDebug(s"Sent message to $shuffleServer to fetch ${blockIds.length} blocks on tag $t" +
          s"in ${System.nanoTime() - startTime} ns")
        }
      }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
    }
  }

  def fetchBlockByStream(shuffleServer: InetSocketAddress, execId: Int,
                         blockId: BlockId, callback: OperationCallback): Unit = {
    val startTime = System.nanoTime()
    val headerSize = workerId.serializedSize + UnsafeUtils.INT_SIZE +
                     UnsafeUtils.INT_SIZE + blockId.serializedSize

    val t = tag.incrementAndGet()

    val buffer = Platform.allocateDirectBuffer(headerSize)
    workerId.serialize(buffer)
    buffer.putInt(t)
    buffer.putInt(execId)
    blockId.serialize(buffer)

    val request = new UcxRequest(null, new UcxStats())
    streamData.put(t, new UcxStreamState(callback, request, Int.MaxValue))

    val address = UnsafeUtils.getAdress(buffer)

    val ep = getConnection(shuffleServer)
    worker.synchronized {
      ep.sendAmNonBlocking(ExternalUcxAmId.FETCH_STREAM, address, headerSize,
                           address, 0, UcpConstants.UCP_AM_SEND_FLAG_EAGER,
                           new UcxCallback() {
        override def onSuccess(request: UcpRequest): Unit = {
          buffer.clear()
          logDebug(s"$workerId sent stream to $execId block $blockId " +
            s"tag $t in ${System.nanoTime() - startTime} ns")
        }
      }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
    }
  }
}