/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.rpc

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ConcurrentLinkedQueue
import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.{UcxAmId, UcxShuffleTransport, UcxWorkerWrapper, UcxShuffleBockId}
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.util.ThreadUtils
import org.apache.spark.util.Utils

class GlobalWorkerRpcThread(globalWorker: UcpWorker, transport: UcxShuffleTransport)
  extends Thread with Logging {
  private[ucx] val conf = transport.ucxShuffleConf
  private[ucx] val handler = UcxWorkerWrapper(globalWorker, transport, false, 0)
  private val taskQueue = new ConcurrentLinkedQueue[Runnable]
  private val waiting = new AtomicBoolean(false)
  private val running = new AtomicBoolean(true)
  private val errorHandler = new UcpEndpointErrorHandler {
    override def onError(ucpEndpoint: UcpEndpoint, errorCode: Int, errorString: String): Unit = {
      if (errorCode == UcsConstants.STATUS.UCS_ERR_CONNECTION_RESET) {
        logWarning(s"Connection closed on ep: $ucpEndpoint")
      } else {
        logError(s"Ep $ucpEndpoint got an error: $errorString")
      }
      handler.connections.filter(x => { x._2 == ucpEndpoint }).foreach(
        x => handler.connections.remove(x._1))
      ucpEndpoint.close()
    }
  }
  private[ucx] val Array(host, port) = conf.listenerAddress.split(":")
  private[ucx] val listener = globalWorker.newListener(new UcpListenerParams()
  .setSockAddr(new InetSocketAddress(Utils.localCanonicalHostName, port.toInt))
  .setConnectionHandler((ucpConnectionRequest: UcpConnectionRequest) => {
    val executorId = ucpConnectionRequest.getClientId()
    handler.connections.getOrElseUpdate(
      executorId, globalWorker.newEndpoint(
        new UcpEndpointParams().setConnectionRequest(ucpConnectionRequest)
        .setPeerErrorHandlingMode().setErrorHandler(errorHandler)
        .setName(s"Endpoint to ${executorId}")))
  }))

  setDaemon(true)
  setName("Global worker progress thread")

  private val replyWorkersThreadPool = ThreadUtils.newDaemonFixedThreadPool(
    conf.numListenerThreads, "UcxListenerThread")

  // Main RPC thread. Submit each RPC request to separate thread and send reply back from separate worker.
  globalWorker.setAmRecvHandler(UcxAmId.FETCH_BLOCK,
  (headerAddress: Long, headerSize: Long, amData: UcpAmData, _: UcpEndpoint) => {
    val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
    val replyTag = header.getInt
    val replyExecutor = header.getLong
    val buffer = UnsafeUtils.getByteBufferView(amData.getDataAddress,
                                               amData.getLength.toInt)
    val blockNum = buffer.remaining() / UcxShuffleBockId.serializedSize
    val blockIds = (0 until blockNum).map(_ => UcxShuffleBockId.deserialize(buffer))
    submit(new Runnable {
      override def run: Unit = {
        val blocks = blockIds.map(transport.registeredBlocks(_))
        handler.handleFetchBlockRequest(blocks, replyTag, replyExecutor)
      }
    })
    UcsConstants.STATUS.UCS_OK
  }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG )

  globalWorker.setAmRecvHandler(UcxAmId.FETCH_STREAM,
  (headerAddress: Long, headerSize: Long, amData: UcpAmData, _: UcpEndpoint) => {
    val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
    val replyTag = header.getInt
    val replyExecutor = header.getLong
    val blockId = UcxShuffleBockId.deserialize(header)
    submit(new Runnable {
      override def run: Unit = {
        val block = transport.registeredBlocks(blockId)
        handler.handleFetchBlockStream(block, replyTag, replyExecutor)
      }
    })
    UcsConstants.STATUS.UCS_OK
  }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG )

  @inline
  def submit(task: Runnable): Unit = {
    replyWorkersThreadPool.submit(task)
  }

  @inline
  def post(task: Runnable): Unit = {
    taskQueue.add(task)
    if (waiting.compareAndSet(true, false)) {
      globalWorker.signal()
    }
  }

  def close(): Unit = {
    listener.close()
    handler.close()
    running.set(false)
    globalWorker.signal()
    globalWorker.close()
  }

  override def run(): Unit = {
    val useWakeup = conf.useWakeup
    while (running.get()) {
      val task = taskQueue.poll()
      if (task != null) {
        task.run()
      }
      while (globalWorker.progress != 0) {}
      if (useWakeup && taskQueue.isEmpty && waiting.compareAndSet(false, true)) {
        globalWorker.waitForEvents()
      }
    }
  }
}
