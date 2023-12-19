/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel
import java.util.concurrent.{ConcurrentLinkedQueue, Callable, Future, FutureTask}
import scala.collection.concurrent.TrieMap
import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants
import org.openucx.jucx.ucs.UcsConstants.MEMORY_TYPE
import org.openucx.jucx.{UcxCallback, UcxException, UcxUtils}
import org.apache.spark.shuffle.ucx.memory.UcxBounceBufferMemoryBlock
import org.apache.spark.shuffle.utils.{UnsafeUtils, UcxLogging}

/**
 * Worker per thread wrapper, that maintains connection and progress logic.
 */
case class ExternalUcxServerWorker(val worker: UcpWorker,
                                   transport: ExternalUcxServerTransport,
                                   workerId: UcxWorkerId)
  extends Closeable with UcxLogging {
  private[this] lazy val shuffleClients = new TrieMap[UcxWorkerId, UcpEndpoint]
  private[this] lazy val memPool = transport.hostBounceBufferMemoryPool
  private[this] lazy val maxReplySize = transport.ucxShuffleConf.maxReplySize

  override def close(): Unit = {
    worker.close()
  }

  def getConnectionBack(shuffleClient: UcxWorkerId): UcpEndpoint = {
    shuffleClients.getOrElseUpdate(shuffleClient, {
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
      connectBack(shuffleClient, appMap(exeWorkerId))
    })
  }

  def connectBack(shuffleClient: UcxWorkerId, workerAddress: ByteBuffer): UcpEndpoint = {
      logDebug(s"$workerId connecting back to $shuffleClient by worker address")
      worker.synchronized {
        worker.newEndpoint(new UcpEndpointParams()
          .setName(s"Server to $UcxWorkerId")
          .setUcpAddress(workerAddress))
      }
  }

  def handleFetchBlockRequest(clientWorker: UcxWorkerId, replyTag: Int, blocks: Seq[(Long, ReadableByteChannel)]): Unit = try {
    val tagAndSizes = UnsafeUtils.INT_SIZE + UnsafeUtils.INT_SIZE * blocks.size
    val resultMemory = memPool.get(tagAndSizes + blocks.map(x => x._1).sum)
      .asInstanceOf[UcxBounceBufferMemoryBlock]
    val resultBuffer = UcxUtils.getByteBufferView(resultMemory.address,
      resultMemory.size)

    resultBuffer.putInt(replyTag)
    for (i <- 0 until blocks.size) {
      resultBuffer.putInt(blocks(i)._1.toInt)
    }

    for (i <- 0 until blocks.size) {
      resultBuffer.limit(resultBuffer.position() + blocks(i)._1.toInt)
      blocks(i)._2.read(resultBuffer)
    }

    val startTime = System.nanoTime()
    val ep = getConnectionBack(clientWorker)
    worker.synchronized {
      ep.sendAmNonBlocking(1, resultMemory.address, tagAndSizes,
      resultMemory.address + tagAndSizes, resultMemory.size - tagAndSizes, 0, new UcxCallback {
        override def onSuccess(request: UcpRequest): Unit = {
          logTrace(s"Sent to ${clientWorker} ${blocks.length} blocks of size: ${resultMemory.size} " +
          s"tag $replyTag in ${System.nanoTime() - startTime} ns.")
          memPool.put(resultMemory)
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

  def handleFetchBlockStream(clientWorker: UcxWorkerId, replyTag: Int,
                             blockInfo: (Long, ReadableByteChannel)): Unit = {
    val headerSize = UnsafeUtils.INT_SIZE + UnsafeUtils.INT_SIZE
    val maxBodySize = maxReplySize - headerSize.toLong
    val blockSlice = (0L until blockInfo._1 by maxBodySize)

    val mem = memPool.get(maxReplySize).asInstanceOf[UcxBounceBufferMemoryBlock]
    val buffer = UcxUtils.getByteBufferView(mem.address, mem.size)

    def send(workerWrapper: ExternalUcxServerWorker, currentId: Int): Unit = try {
      val remaining = blockSlice.length - currentId - 1
      val currentOffset = blockSlice(currentId)
      val currentSize = (blockInfo._1 - currentOffset).min(maxBodySize)
      buffer.clear()
      buffer.limit(headerSize + currentSize.toInt)
      buffer.putInt(replyTag)
      buffer.putInt(remaining)
      blockInfo._2.read(buffer)

      val startTime = System.nanoTime()
      val ep = workerWrapper.getConnectionBack(clientWorker)
      workerWrapper.worker.synchronized {
        ep.sendAmNonBlocking(2, mem.address, headerSize,
          mem.address + headerSize, currentSize, 0, new UcxCallback {
            override def onSuccess(request: UcpRequest): Unit = {
              logTrace(s"Reply stream block $currentId size $currentSize tag " +
                s"$replyTag in ${System.nanoTime() - startTime} ns.")
              if (remaining > 0) {
                transport.submit(new Runnable {
                  override def run = send(transport.selectWorker, currentId + 1)
                })
              } else {
                mem.close()
                blockInfo._2.close()
              }
            }
            override def onError(ucsStatus: Int, errorMsg: String): Unit = {
              logError(s"Failed to reply stream $errorMsg")
              mem.close()
              blockInfo._2.close()
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
