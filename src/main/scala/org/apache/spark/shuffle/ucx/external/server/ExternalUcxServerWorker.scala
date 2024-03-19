/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.concurrent.{ConcurrentLinkedQueue, ConcurrentHashMap, CountDownLatch, Future, FutureTask}
import scala.collection.mutable
import scala.collection.JavaConverters._
import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants
import org.openucx.jucx.ucs.UcsConstants.MEMORY_TYPE
import org.openucx.jucx.{UcxCallback, UcxException, UcxUtils}
import org.apache.spark.shuffle.ucx.memory.UcxLinkedMemBlock
import org.apache.spark.shuffle.utils.{UnsafeUtils, UcxLogging}
import java.net.InetSocketAddress

/**
 * Worker per thread wrapper, that maintains connection and progress logic.
 */
case class ExternalUcxServerWorker(val worker: UcpWorker,
                                   transport: ExternalUcxServerTransport,
                                   workerId: UcxWorkerId,
                                   port: Int)
  extends Closeable with UcxLogging {
  private[this] val memPool = transport.hostBounceBufferMemoryPool
  private[this] val maxReplySize = transport.ucxShuffleConf.maxReplySize
  private[this] val shuffleClients = new ConcurrentHashMap[UcxWorkerId, UcpEndpoint]
  private[ucx] val executor = new UcxWorkerThread(
    worker, transport.ucxShuffleConf.useWakeup)

  private val endpoints = mutable.Set.empty[UcpEndpoint]
  private val listener = worker.newListener(
    new UcpListenerParams().setSockAddr(new InetSocketAddress("0.0.0.0", port))
    .setConnectionHandler((ucpConnectionRequest: UcpConnectionRequest) => {
      val clientAddress = ucpConnectionRequest.getClientAddress()
      val ep = worker.newEndpoint(
        new UcpEndpointParams().setConnectionRequest(ucpConnectionRequest)
          .setPeerErrorHandlingMode().setErrorHandler(errorHandler)
          .setName(s"Endpoint to $clientAddress"))
      endpoints.add(ep)
    }))

  private val listenerAddress = listener.getAddress
  private val errorHandler = new UcpEndpointErrorHandler {
    override def onError(ucpEndpoint: UcpEndpoint, errorCode: Int, errorString: String): Unit = {
      if (errorCode == UcsConstants.STATUS.UCS_ERR_CONNECTION_RESET) {
        logWarning(s"Connection closed on ep: $ucpEndpoint")
      } else {
        logError(s"Ep $ucpEndpoint got an error: $errorString")
      }
      endpoints.remove(ucpEndpoint)
      ucpEndpoint.close()
    }
  }

  // Main RPC thread. Submit each RPC request to separate thread and send reply back from separate worker.
  worker.setAmRecvHandler(ExternalAmId.FETCH_BLOCK,
    (headerAddress: Long, headerSize: Long, amData: UcpAmData, _: UcpEndpoint) => {
    val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
    val shuffleClient = UcxWorkerId.deserialize(header)
    val replyTag = header.getInt
    val exeId = header.getInt
    val buffer = UnsafeUtils.getByteBufferView(amData.getDataAddress,
                                               amData.getLength.toInt)
    val BlockNum = buffer.remaining() / UcxShuffleBlockId.serializedSize
    val blockIds = (0 until BlockNum).map(
      _ => UcxShuffleBlockId.deserialize(buffer))
    logTrace(s"Recv fetch from $shuffleClient tag $replyTag.")
    transport.handleFetchBlockRequest(this, shuffleClient, exeId, replyTag, blockIds)
    UcsConstants.STATUS.UCS_OK
  }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG )

  // Main RPC thread. Submit each RPC request to separate thread and send stream back from separate worker.
  worker.setAmRecvHandler(ExternalAmId.FETCH_STREAM,
    (headerAddress: Long, headerSize: Long, amData: UcpAmData, _: UcpEndpoint) => {
    val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
    val shuffleClient = UcxWorkerId.deserialize(header)
    val replyTag = header.getInt
    val exeId = header.getInt
    val blockId = UcxShuffleBlockId.deserialize(header)
    logTrace(s"Recv stream from $shuffleClient tag $replyTag.")
    transport.handleFetchBlockStream(this, shuffleClient, exeId, replyTag, blockId)
    UcsConstants.STATUS.UCS_OK
  }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG )

  // AM to get worker address for client worker and connect server workers to it
  worker.setAmRecvHandler(ExternalAmId.CONNECT,
    (headerAddress: Long, headerSize: Long, amData: UcpAmData, _: UcpEndpoint) => {
    val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
    val shuffleClient = UcxWorkerId.deserialize(header)
    val workerAddress = UnsafeUtils.getByteBufferView(amData.getDataAddress,
                                                      amData.getLength.toInt)
    val copiedAddress = ByteBuffer.allocateDirect(workerAddress.remaining)
    copiedAddress.put(workerAddress)
    connected(shuffleClient, copiedAddress)
    transport.handleConnect(this, shuffleClient, copiedAddress)
    UcsConstants.STATUS.UCS_OK
  }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG )
  // Main RPC thread. reply with ucpAddress.

  // AM to get worker address for client worker and connect server workers to it
  worker.setAmRecvHandler(ExternalAmId.ADDRESS,
    (headerAddress: Long, headerSize: Long, amData: UcpAmData, ep: UcpEndpoint) => {
    handleAddress(ep)
    UcsConstants.STATUS.UCS_OK
  }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG )
  // Main RPC thread. reply with ucpAddress.

  def start(): Unit = {
    executor.start()
  }

  override def close(): Unit = {
    if (!shuffleClients.isEmpty) {
      logInfo(s"$workerId closing ${shuffleClients.size} clients")
      shuffleClients.values.asScala.map(
        _.closeNonBlockingFlush()).foreach(req =>
          while (!req.isCompleted){
            worker.progress()
          }
        )
    }
    if (!endpoints.isEmpty) {
      logInfo(s"$workerId closing ${endpoints.size} eps")
      endpoints.map(
        _.closeNonBlockingForce()).foreach(req =>
          while (!req.isCompleted){
            worker.progress()
          }
        )
    }
    listener.close()
  }

  def closing(): Future[Unit.type] = {
    val cleanTask = new FutureTask(new Runnable {
      override def run() = close()
    }, Unit)
    executor.close(cleanTask)
    cleanTask
  }

  def getPort(): Int = {
    listenerAddress.getPort()
  }

  def getAddress(): InetSocketAddress = {
    listenerAddress
  }

  @`inline`
  private def doDisconnect(workerId: UcxWorkerId): Unit = {
    try {
      Option(shuffleClients.remove(workerId)).foreach(ep => {
        ep.closeNonBlockingFlush()
      })
    } catch {
      case e: Throwable => logWarning(s"$workerId disconnect $e")
    }
  }

  @`inline`
  def disconnect(workerIds: Seq[UcxWorkerId]): Unit = {
    executor.post(() => workerIds.foreach(doDisconnect(_)))
  }

  @`inline`
  def connected(shuffleClient: UcxWorkerId, workerAddress: ByteBuffer): Unit = {
    logDebug(s"$workerId connecting back to $shuffleClient by worker address")
    shuffleClients.computeIfAbsent(shuffleClient, _ => {
      worker.newEndpoint(new UcpEndpointParams()
        .setErrorHandler(new UcpEndpointErrorHandler {
          override def onError(ucpEndpoint: UcpEndpoint, errorCode: Int, errorString: String): Unit = {
            shuffleClients.remove(shuffleClient)
            ucpEndpoint.close()
          }
        })
        .setName(s"Server to $shuffleClient")
        .setUcpAddress(workerAddress))
    })
  }

  def awaitConnection(shuffleClient: UcxWorkerId): UcpEndpoint = {
    shuffleClients.getOrDefault(shuffleClient, {
      // wait until connected finished
      val startTime = System.currentTimeMillis()
      while (!shuffleClients.containsKey(shuffleClient)) {
        if  (System.currentTimeMillis() - startTime > 10000) {
          throw new UcxException(s"Don't get a worker address for $shuffleClient")
        }
        Thread.`yield`
      }
      shuffleClients.get(shuffleClient)
    })
  }

  def handleAddress(ep: UcpEndpoint) = {
    val msg = transport.getServerPortsBuffer()
    val header = UnsafeUtils.getAdress(msg)
    ep.sendAmNonBlocking(
      ExternalAmId.REPLY_ADDRESS, header, msg.remaining(), header, 0,
      UcpConstants.UCP_AM_SEND_FLAG_EAGER, new UcxCallback {
        override def onSuccess(request: UcpRequest): Unit = {
          logTrace(s"$workerId sent to REPLY_ADDRESS to $ep")
          msg.clear()
        }

        override def onError(ucsStatus: Int, errorMsg: String): Unit = {
          logWarning(s"$workerId sent to REPLY_ADDRESS to $ep: $errorMsg")
        }
      }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
  }

  def handleFetchBlockRequest(
    clientWorker: UcxWorkerId, replyTag: Int,
    blockInfos: Seq[(FileChannel, Long, Long)]): Unit = {
    if (blockInfos.length == 1 && blockInfos(0)._3 > maxReplySize) {
      return handleFetchBlockStream(clientWorker, replyTag, blockInfos(0),
                                    ExternalAmId.REPLY_SLICE)
    }

    val tagAndSizes = UnsafeUtils.INT_SIZE + UnsafeUtils.INT_SIZE * blockInfos.length
    val msgSize = tagAndSizes + blockInfos.map(x => x._3).sum
    val resultMemory = memPool.get(msgSize).asInstanceOf[UcxLinkedMemBlock]
    val resultBuffer = UcxUtils.getByteBufferView(resultMemory.address, msgSize)

    resultBuffer.putInt(replyTag)
    val blocksRange = 0 until blockInfos.length
    for (i <- blocksRange) {
      resultBuffer.putInt(blockInfos(i)._3.toInt)
    }

    for (i <- blocksRange) {
      val (blockCh, blockOffset, blockSize) = blockInfos(i)
      resultBuffer.limit(resultBuffer.position() + blockSize.toInt)
      blockCh.read(resultBuffer, blockOffset)
    }

    val startTime = System.nanoTime()
    val ep = awaitConnection(clientWorker)
    executor.post(() => {
      ep.sendAmNonBlocking(ExternalAmId.REPLY_BLOCK, resultMemory.address, tagAndSizes,
      resultMemory.address + tagAndSizes, msgSize - tagAndSizes, 0, new UcxCallback {
        override def onSuccess(request: UcpRequest): Unit = {
          logTrace(s"Sent to ${clientWorker} ${blockInfos.length} blocks of size: " +
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
  }

  def handleFetchBlockStream(clientWorker: UcxWorkerId, replyTag: Int,
                             blockInfo: (FileChannel, Long, Long),
                             amId: Int = ExternalAmId.REPLY_STREAM): Unit = {
    val headerSize = UnsafeUtils.INT_SIZE + UnsafeUtils.INT_SIZE
    val maxBodySize = maxReplySize - headerSize.toLong
    val (blockCh, blockOffset, blockSize) = blockInfo
    val blockSlice = (0L until blockSize by maxBodySize)
    val firstLatch = new CountDownLatch(0)

    def send(workerWrapper: ExternalUcxServerWorker, currentId: Int,
             sendLatch: CountDownLatch): Unit = try {
      val currentOffset = blockSlice(currentId)
      val currentSize = (blockSize - currentOffset).min(maxBodySize)
      val msgSize = headerSize + currentSize.toInt

      val mem = memPool.get(msgSize).asInstanceOf[UcxLinkedMemBlock]
      val buffer = mem.toByteBuffer()

      val remaining = blockSlice.length - currentId - 1

      buffer.limit(msgSize)
      buffer.putInt(replyTag)
      buffer.putInt(remaining)
      blockCh.read(buffer, blockOffset + currentOffset)

      val nextLatch = new CountDownLatch(1)
      if (remaining > 0) {
        transport.submit(() => send(this, currentId + 1, nextLatch))
      }
      sendLatch.await()

      val startTime = System.nanoTime()
      val ep = workerWrapper.awaitConnection(clientWorker)
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
    } catch {
      case ex: Throwable =>
        logError(s"Failed to reply stream $clientWorker tag $replyTag $currentId $ex.")
    }

    send(this, 0, firstLatch)
  }
}
