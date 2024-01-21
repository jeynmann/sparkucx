/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, Callable, Future, FutureTask}
import scala.collection.concurrent.TrieMap
import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants
import org.openucx.jucx.ucs.UcsConstants.MEMORY_TYPE
import org.openucx.jucx.{UcxCallback, UcxException, UcxUtils}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.shuffle.ucx.memory.UcxLinkedMemBlock
import org.apache.spark.shuffle.utils.{UnsafeUtils, UcxLogging}

/**
 * Worker per thread wrapper, that maintains connection and progress logic.
 */
case class ExternalUcxServerWorker(val worker: UcpWorker,
                                   transport: ExternalUcxServerTransport,
                                   workerId: UcxWorkerId)
  extends Closeable with UcxLogging {
  private[this] val memPool = transport.hostBounceBufferMemoryPool
  private[this] val maxReplySize = transport.ucxShuffleConf.maxReplySize
  private[this] val shuffleClients = new TrieMap[UcxWorkerId, UcpEndpoint]
  private[ucx] val executor = new UcxWorkerThread(
    worker, transport.ucxShuffleConf.useWakeup)

  // Main RPC thread. Submit each RPC request to separate thread and send reply back from separate worker.
  worker.setAmRecvHandler(ExternalAmId.FETCH_BLOCK,
    (headerAddress: Long, headerSize: Long, amData: UcpAmData, _: UcpEndpoint) => {
    val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
    val workerId = UcxWorkerId.deserialize(header)
    val replyTag = header.getInt
    val exeId = header.getInt
    val buffer = UnsafeUtils.getByteBufferView(amData.getDataAddress,
                                               amData.getLength.toInt)
    val BlockNum = buffer.remaining() / UcxShuffleBlockId.serializedSize
    val blockIds = (0 until BlockNum).map(
      _ => UcxShuffleBlockId.deserialize(buffer))
    transport.handleFetchBlockRequest(workerId, exeId, replyTag, blockIds)
    UcsConstants.STATUS.UCS_OK
  }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG )

  // Main RPC thread. Submit each RPC request to separate thread and send stream back from separate worker.
  worker.setAmRecvHandler(ExternalAmId.FETCH_STREAM,
    (headerAddress: Long, headerSize: Long, amData: UcpAmData, _: UcpEndpoint) => {
    val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
    val workerId = UcxWorkerId.deserialize(header)
    val replyTag = header.getInt
    val exeId = header.getInt
    val blockId = UcxShuffleBlockId.deserialize(header)
    transport.handleFetchBlockStream(workerId, exeId, replyTag, blockId)
    UcsConstants.STATUS.UCS_OK
  }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG )

  // AM to get worker address for client worker and connect server workers to it
  worker.setAmRecvHandler(ExternalAmId.CONNECT,
    (headerAddress: Long, headerSize: Long, amData: UcpAmData, _: UcpEndpoint) => {
    val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
    val workerId = UcxWorkerId.deserialize(header)
    val workerAddress = UnsafeUtils.getByteBufferView(amData.getDataAddress,
                                                      amData.getLength.toInt)
    val copiedAddress = ByteBuffer.allocateDirect(workerAddress.remaining)
    copiedAddress.put(workerAddress)
    transport.handleConnect(workerId, copiedAddress)
    UcsConstants.STATUS.UCS_OK
  }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG )
  // Main RPC thread. reply with ucpAddress.

  def start(): Unit = {
    executor.start()
  }

  override def close(): Unit = {
    reportClients()
    executor.close()
  }

  def close(closeCb: Runnable): Unit = {
    executor.post(closeCb)
    executor.close()
  }

  def reportClients(): Unit = {
    if (!shuffleClients.isEmpty) {
      logInfo(s"$workerId clients ${shuffleClients.size}")
    }
  }

  def doDisconnect(workerId: UcxWorkerId): Unit = {
    shuffleClients.remove(workerId).map(_.close)
  }

  def disconnect(workerIds: Seq[UcxWorkerId]): Unit = {
    executor.post(() => workerIds.foreach(doDisconnect(_)))
  }

  def connectBack(shuffleClient: UcxWorkerId, workerAddress: ByteBuffer): Future[UcpEndpoint] = {
    logDebug(s"$workerId connecting back to $shuffleClient by worker address")
    val f = new FutureTask(new Callable[UcpEndpoint] {
      override def call = {
        shuffleClients.getOrElseUpdate(shuffleClient, {
          worker.newEndpoint(new UcpEndpointParams()
            .setName(s"Server to $UcxWorkerId")
            .setUcpAddress(workerAddress))
        })
      }
    })
    executor.post(f)
    f
  }

  def getConnectionBack(shuffleClient: UcxWorkerId): UcpEndpoint = {
    if (shuffleClients.contains(shuffleClient)) {
      shuffleClients(shuffleClient)
    } else {
      // wait until transport handleConnect finished
      val exeWorkerId = UcxWorkerId.makeExeWorkerId(shuffleClient)
      val workerMap = transport.workerMap
      if (!workerMap.contains(shuffleClient.appId)) {
        val startTime = System.currentTimeMillis()
        while (!workerMap.contains(shuffleClient.appId)) {
          if  (System.currentTimeMillis() - startTime > 10000) {
            throw new UcxException(s"Don't get a worker address for $UcxWorkerId")
          }
          Thread.`yield`
        }
      }
      val appMap = workerMap(shuffleClient.appId)
      if (!appMap.contains(exeWorkerId)) {
        val startTime = System.currentTimeMillis()
        while (!appMap.contains(exeWorkerId)) {
          if  (System.currentTimeMillis() - startTime > 10000) {
            throw new UcxException(s"Don't get a worker address for $UcxWorkerId")
          }
          Thread.`yield`
        }
      }
      connectBack(shuffleClient, appMap(exeWorkerId)).get()
    }
  }

  def handleFetchBlockRequest(clientWorker: UcxWorkerId, replyTag: Int,
                              blocks: Seq[(Long, ManagedBuffer)]): Unit = try {
    if (blocks.size == 1 && blocks(0)._1 > maxReplySize) {
      return handleFetchBlockStream(clientWorker, replyTag, blocks(0),
                                    ExternalAmId.REPLY_SLICE)
    }

    val tagAndSizes = UnsafeUtils.INT_SIZE + UnsafeUtils.INT_SIZE * blocks.size
    val msgSize = tagAndSizes + blocks.map(x => x._1).sum
    val resultMemory = memPool.get(msgSize).asInstanceOf[UcxLinkedMemBlock]
    val resultBuffer = UcxUtils.getByteBufferView(resultMemory.address, msgSize)
    val blockCh = blocks.map(x => Channels.newChannel(x._2.createInputStream()))

    resultBuffer.putInt(replyTag)
    for (i <- 0 until blocks.size) {
      resultBuffer.putInt(blocks(i)._1.toInt)
    }

    for (i <- 0 until blocks.size) {
      resultBuffer.limit(resultBuffer.position() + blocks(i)._1.toInt)
      blockCh(i).read(resultBuffer)
    }

    val startTime = System.nanoTime()
    val ep = getConnectionBack(clientWorker)
    executor.post(() => {
      ep.sendAmNonBlocking(ExternalAmId.REPLY_BLOCK, resultMemory.address, tagAndSizes,
      resultMemory.address + tagAndSizes, msgSize - tagAndSizes, 0, new UcxCallback {
        override def onSuccess(request: UcpRequest): Unit = {
          logTrace(s"Sent to ${clientWorker} ${blocks.length} blocks of size: " +
          s"${msgSize} tag $replyTag in ${System.nanoTime() - startTime} ns.")
          resultMemory.close()
        }
        
        override def onError(ucsStatus: Int, errorMsg: String): Unit = {
          logError(s"Failed to send $errorMsg")
          resultMemory.close()
        }
      }, new UcpRequestParams().setMemoryType(UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
      .setMemoryHandle(resultMemory.memory))
    })

    blockCh.foreach(_.close())
  } catch {
    case ex: Throwable => logError(s"Failed to reply block tag $replyTag $ex.")
  }

  def handleFetchBlockStream(clientWorker: UcxWorkerId, replyTag: Int,
                             blockInfo: (Long, ManagedBuffer),
                             amId: Int = ExternalAmId.REPLY_STREAM): Unit = {
    val headerSize = UnsafeUtils.INT_SIZE + UnsafeUtils.INT_SIZE
    val maxBodySize = maxReplySize - headerSize.toLong
    val blockSlice = (0L until blockInfo._1 by maxBodySize)
    val blockCh = Channels.newChannel(blockInfo._2.createInputStream())
    val firstLatch = new CountDownLatch(1)

    def send(workerWrapper: ExternalUcxServerWorker, currentId: Int,
             sendLatch: CountDownLatch): Unit = try {
      val currentOffset = blockSlice(currentId)
      val currentSize = (blockInfo._1 - currentOffset).min(maxBodySize)
      val msgSize = headerSize + currentSize.toInt

      val mem = memPool.get(msgSize).asInstanceOf[UcxLinkedMemBlock]
      val buffer = mem.toByteBuffer()

      val remaining = blockSlice.length - currentId - 1

      buffer.limit(msgSize)
      buffer.putInt(replyTag)
      buffer.putInt(remaining)
      blockCh.read(buffer)

      val nextLatch = new CountDownLatch(1)
      if (remaining > 0) {
        transport.submit(() => send(transport.selectWorker, currentId + 1,
                                    nextLatch))
      }
      sendLatch.await()

      val startTime = System.nanoTime()
      val ep = workerWrapper.getConnectionBack(clientWorker)
      workerWrapper.executor.post(() => {
        ep.sendAmNonBlocking(amId, mem.address, headerSize,
          mem.address + headerSize, currentSize, 0, new UcxCallback {
            override def onSuccess(request: UcpRequest): Unit = {
              logTrace(s"Reply stream size $currentSize tag $replyTag seg " +
                s"$currentId in ${System.nanoTime() - startTime} ns.")
              mem.close()
              nextLatch.countDown()
            }
            override def onError(ucsStatus: Int, errorMsg: String): Unit = {
              logError(s"Failed to reply tag $replyTag seg $currentId $errorMsg")
              mem.close()
              nextLatch.countDown()
            }
          }, new UcpRequestParams()
            .setMemoryType(UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
            .setMemoryHandle(mem.memory))
      })

      if (remaining == 0) {
        blockCh.close()
      }
    } catch {
      case ex: Throwable =>
        logError(s"Failed to reply stream $currentId tag $replyTag $ex.")
    }

    firstLatch.countDown()
    send(this, 0, firstLatch)
  }
}
