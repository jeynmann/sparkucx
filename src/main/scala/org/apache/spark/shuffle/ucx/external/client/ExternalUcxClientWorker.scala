/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import java.io.Closeable
import java.nio.channels.ReadableByteChannel
import java.util.concurrent.{ConcurrentLinkedQueue, ConcurrentHashMap, Future, FutureTask}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap
import scala.collection.JavaConverters._
import scala.util.Random
import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants
import org.openucx.jucx.ucs.UcsConstants.MEMORY_TYPE
import org.openucx.jucx.{UcxCallback, UcxException, UcxUtils}
import org.apache.spark.shuffle.ucx.memory.UcxSharedMemoryBlock
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
  private[this] val tag = new AtomicInteger(Random.nextInt())
  private[this] val memPool = transport.hostBounceBufferMemoryPool
  private[this] val connectQueue = new ConcurrentLinkedQueue[InetSocketAddress]
  private[this] val shuffleServers = new ConcurrentHashMap[String, UcpEndpoint]
  private[this] val executor = new UcxWorkerThread(
    worker, transport.ucxShuffleConf.useWakeup)
  private[this] lazy val requestData = new TrieMap[Int, (Seq[OperationCallback], UcxRequest)]
  private[this] lazy val streamData = new TrieMap[Int, UcxStreamState]
  private[this] lazy val sliceData = new TrieMap[Int, UcxSliceState]

  // Receive block data handler
  worker.setAmRecvHandler(ExternalAmId.REPLY_SLICE,
    (headerAddress: Long, headerSize: Long, ucpAmData: UcpAmData,
    ep: UcpEndpoint) => {
      val headerBuffer = UnsafeUtils.getByteBufferView(headerAddress,
                                                       headerSize.toInt)
      val i = headerBuffer.getInt
      val remaining = headerBuffer.getInt

      handleReplySlice(i, remaining, ucpAmData)
      UcsConstants.STATUS.UCS_OK
    }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG)

  worker.setAmRecvHandler(ExternalAmId.REPLY_STREAM,
    (headerAddress: Long, headerSize: Long, ucpAmData: UcpAmData,
    _: UcpEndpoint) => {
      val headerBuffer = UnsafeUtils.getByteBufferView(headerAddress,
                                                       headerSize.toInt)
      val i = headerBuffer.getInt
      val remaining = headerBuffer.getInt

      handleReplyStream(i, remaining, ucpAmData)
      UcsConstants.STATUS.UCS_OK
    }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG)

  worker.setAmRecvHandler(ExternalAmId.REPLY_BLOCK,
    (headerAddress: Long, headerSize: Long, ucpAmData: UcpAmData, _: UcpEndpoint) => {
      val headerBuffer = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
      val i = headerBuffer.getInt
      // Header contains tag followed by sizes of blocks
      val numBlocks = headerBuffer.remaining() / UnsafeUtils.INT_SIZE
      val blockSizes = (0 until numBlocks).map(_ => headerBuffer.getInt())

      handleReplyBlock(i, blockSizes, ucpAmData)
      if (ucpAmData.isDataValid) {
        UcsConstants.STATUS.UCS_INPROGRESS
      } else {
        UcsConstants.STATUS.UCS_OK
      }
    }, UcpConstants.UCP_AM_FLAG_PERSISTENT_DATA | UcpConstants.UCP_AM_FLAG_WHOLE_MSG)

  worker.setAmRecvHandler(ExternalAmId.REPLY_ADDRESS,
    (headerAddress: Long, headerSize: Long, _: UcpAmData, _: UcpEndpoint) => {
      val headerBuffer = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
      val copiedBuffer = ByteBuffer.allocateDirect(headerBuffer.remaining())

      copiedBuffer.put(headerBuffer)
      copiedBuffer.rewind()

      transport.handleReplyAddress(copiedBuffer)
      UcsConstants.STATUS.UCS_OK
    }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG)

  private def handleReplySlice(
    i: Int, remaining: Int, ucpAmData: UcpAmData): Unit = {
      val sliceState = sliceData.getOrElseUpdate(i, {
        requestData.remove(i) match {
          case Some(data) => {
            val mem = memPool.get(ucpAmData.getLength * (remaining + 1))
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
    }

  private def handleReplyStream(
    i: Int, remaining: Int, ucpAmData: UcpAmData): Unit = {
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
    }

  private def handleReplyBlock(
    i: Int, blockSizes: Seq[Int], ucpAmData: UcpAmData): Unit = {
      val data = requestData.remove(i)

      if (data.isEmpty) {
        throw new UcxException(s"No data for tag $i.")
      }

      val (callbacks, request) = data.get
      val stats = request.getStats.get.asInstanceOf[UcxStats]
      stats.receiveSize = ucpAmData.getLength

      val numBlocks = blockSizes.length

      var offset = 0
      val refCounts = new AtomicInteger(numBlocks)
      if (ucpAmData.isDataValid) {
        request.completed = true
        stats.endTime = System.nanoTime()
        logDebug(s"Received amData: $ucpAmData for tag $i " +
          s"in ${stats.getElapsedTimeNs} ns")

        val closeCb = () => executor.post(() => ucpAmData.close())
        val address = ucpAmData.getDataAddress
        for (b <- 0 until numBlocks) {
          val blockSize = blockSizes(b)
          if (callbacks(b) != null) {
            callbacks(b).onComplete(new UcxSucceedOperationResult(
              new UcxSharedMemoryBlock(closeCb, refCounts, address + offset,
                                       blockSize), stats))
            offset += blockSize
          }
        }
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
                val blockSize = blockSizes(b)
                callbacks(b).onComplete(new UcxSucceedOperationResult(
                  new UcxRefCountMemoryBlock(mem, offset, blockSize, refCounts),
                  stats))
                offset += blockSize
              }

            }
          }, UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST))
      }
    }

  def start(): Unit = {
    executor.start()
  }

  override def close(): Unit = {
    val closeRequests = shuffleServers.asScala.map {
      case (_, endpoint) => endpoint.closeNonBlockingForce()
    }
    while (!closeRequests.forall(_.isCompleted)) {
      progress()
    }
  }

  def closing(): Future[Unit.type] = {
    val cleanTask = new FutureTask(new Runnable {
      override def run() = close()
    }, Unit)
    executor.close(cleanTask)
    cleanTask
  }

  /**
   * The only place for worker progress
   */
  def progress(): Int = worker.synchronized {
    worker.progress()
  }

  @`inline`
  def requestAddress(localServer: InetSocketAddress): Unit = {
    executor.post(() => shuffleServers.computeIfAbsent("0.0.0.0", _ => {
      doConnect(localServer, ExternalAmId.ADDRESS,
                UcpConstants.UCP_AM_SEND_FLAG_REPLY)
    }))
  }

  @`inline`
  def connect(shuffleServer: InetSocketAddress): Unit = {
    connectQueue.add(shuffleServer)
  }

  @`inline`
  def connectAll(addressSet: Seq[InetSocketAddress]): Unit = {
    addressSet.foreach(connectQueue.add(_))
  }

  @`inline`
  private def doConnectNext(): Unit = {
    if (!connectQueue.isEmpty) {
      getConnection(connectQueue.poll())
    }
  }

  private def doConnect(shuffleServer: InetSocketAddress,
                        amId: Int = ExternalAmId.CONNECT,
                        flag: Long = 0): UcpEndpoint = {
    val endpointParams = new UcpEndpointParams().setPeerErrorHandlingMode()
      .setSocketAddress(shuffleServer).sendClientId()
      .setErrorHandler(new UcpEndpointErrorHandler() {
        override def onError(ep: UcpEndpoint, status: Int, errorMsg: String): Unit = {
          logError(s"Endpoint to $shuffleServer got an error: $errorMsg")
          shuffleServers.remove(shuffleServer.getHostName())
        }
      }).setName(s"Client to $shuffleServer")

    logDebug(s"$workerId connecting to external service $shuffleServer")

    val header = Platform.allocateDirectBuffer(workerId.serializedSize)
    workerId.serialize(header)
    header.rewind()
    val workerAddress = worker.getAddress

    val ep = worker.newEndpoint(endpointParams)
    ep.sendAmNonBlocking(
      amId, UcxUtils.getAddress(header), header.remaining(),
      UcxUtils.getAddress(workerAddress), workerAddress.remaining(),
      UcpConstants.UCP_AM_SEND_FLAG_EAGER | flag, new UcxCallback() {
        override def onSuccess(request: UcpRequest): Unit = {
          header.clear()
          workerAddress.clear()
          doConnectNext()
        }
        override def onError(ucsStatus: Int, errorMsg: String): Unit = {
          doConnectNext()
        }
      }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
    ep
  }

  private def getConnection(shuffleServer: InetSocketAddress): UcpEndpoint = {
    shuffleServers.computeIfAbsent(shuffleServer.getHostName(), _ => {
      doConnect(shuffleServer)
    })
  }

  private def getConnection(host: String): UcpEndpoint = {
    shuffleServers.computeIfAbsent(host, _ => {
      val shuffleServer = transport.ucxServers.computeIfAbsent(host, _ => {
        logInfo(s"connecting $host with controller port")
        new InetSocketAddress(host, transport.ucxServerPort)
      })
      doConnect(shuffleServer)
    })
  }

  def fetchBlocksByBlockIds(host: String, execId: Int, blockIds: Seq[BlockId],
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

    executor.post(() => {
      val ep = getConnection(host)
      ep.sendAmNonBlocking(ExternalAmId.FETCH_BLOCK, address,
      headerSize, dataAddress, buffer.capacity() - headerSize,
      UcpConstants.UCP_AM_SEND_FLAG_EAGER, new UcxCallback() {
        override def onSuccess(request: UcpRequest): Unit = {
          buffer.clear()
          logDebug(s"Sent fetch to $host tag $t blocks ${blockIds.length} " +
          s"in ${System.nanoTime() - startTime} ns")
        }
        override def onError(ucsStatus: Int, errorMsg: String): Unit = {
          val err = s"Sent fetch to $host tag $t failed: $errorMsg";
          callbacks.foreach(_.onFailure(new UcxException(err)))
          requestData.remove(t)
          logError(err)
        }
      }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
    })
  }

  def fetchBlockByStream(host: String, execId: Int, blockId: BlockId,
                         callback: OperationCallback): Unit = {
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

    executor.post(() => {
      val ep = getConnection(host)
      ep.sendAmNonBlocking(ExternalAmId.FETCH_STREAM, address, headerSize,
        address, 0, UcpConstants.UCP_AM_SEND_FLAG_EAGER, new UcxCallback() {
        override def onSuccess(request: UcpRequest): Unit = {
          buffer.clear()
          logDebug(s"Sent stream to $host tag $t block $blockId " +
            s"in ${System.nanoTime() - startTime} ns")
        }
        override def onError(ucsStatus: Int, errorMsg: String): Unit = {
          val err = s"Sent stream to $host tag $t failed: $errorMsg";
          callback.onFailure(new UcxException(err))
          requestData.remove(t)
          logError(err)
        }
      }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
    })
  }
}