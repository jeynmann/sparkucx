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

  // def debugClients(): Unit = {
  //   logDebug(s"$workerId clients ${shuffleClients.size}")
  // }

  def disconnect(workerId: UcxWorkerId): Unit = {
    worker.synchronized {
      shuffleClients.remove(workerId).map(ep =>
        try {
          ep.closeNonBlockingForce()
        } catch {
          case e: Exception => logInfo(s"$workerId close $e")
        })
    }
  }

  def disconnect(workerIds: Seq[UcxWorkerId]): Unit = {
    worker.synchronized {
      workerIds.foreach(disconnect(_))
    }
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

  def handleFetchBlockRequest(clientWorker: UcxWorkerId, replyTag: Int,
                              blocks: Seq[(Long, ManagedBuffer)]): Unit = try {
    if (blocks.size == 1 && blocks(0)._1 > maxReplySize) {
      return handleFetchBlockStream(clientWorker, replyTag, blocks(0), 3)
    }

    val tagAndSizes = UnsafeUtils.INT_SIZE + UnsafeUtils.INT_SIZE * blocks.size
    val resultMemory = memPool.get(tagAndSizes + blocks.map(x => x._1).sum)
      .asInstanceOf[UcxBounceBufferMemoryBlock]
    val resultBuffer = UcxUtils.getByteBufferView(resultMemory.address,
      resultMemory.size)
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

    blockCh.foreach(_.close())
  } catch {
    case ex: Throwable => logError(s"Failed to read and send data: $ex")
  }

  def handleFetchBlockStream(clientWorker: UcxWorkerId, replyTag: Int,
                             blockInfo: (Long, ManagedBuffer), amId: Int = 2)
                             : Unit = {
    val headerSize = UnsafeUtils.INT_SIZE + UnsafeUtils.INT_SIZE
    val maxBodySize = maxReplySize - headerSize.toLong
    val blockSlice = (0L until blockInfo._1 by maxBodySize)
    val blockCh = Channels.newChannel(blockInfo._2.createInputStream())
    val firstLatch = new CountDownLatch(1)

    def send(workerWrapper: ExternalUcxServerWorker, currentId: Int,
             sendLatch: CountDownLatch): Unit = try {
      val mem = memPool.get(maxReplySize).asInstanceOf[UcxBounceBufferMemoryBlock]
      val buffer = UcxUtils.getByteBufferView(mem.address, maxReplySize)

      val remaining = blockSlice.length - currentId - 1
      val currentOffset = blockSlice(currentId)
      val currentSize = (blockInfo._1 - currentOffset).min(maxBodySize)

      buffer.limit(headerSize + currentSize.toInt)
      buffer.putInt(replyTag)
      buffer.putInt(remaining)
      blockCh.read(buffer)

      val nextLatch = new CountDownLatch(1)
      sendLatch.await()

      val startTime = System.nanoTime()
      val ep = workerWrapper.getConnectionBack(clientWorker)
      workerWrapper.worker.synchronized {
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
      }
      
      if (remaining > 0) {
        transport.submit(new Runnable {
          override def run = send(transport.selectWorker, currentId + 1,
                                  nextLatch)
        })
      } else {
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
