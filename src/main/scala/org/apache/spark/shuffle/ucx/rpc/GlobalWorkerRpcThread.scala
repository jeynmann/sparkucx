/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.rpc

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ConcurrentLinkedQueue
import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.{UcxAmId, UcxShuffleTransport, UcxWorkerWrapper, UcxShuffleBockId}
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.util.ThreadUtils

class GlobalWorkerRpcThread(globalWorker: UcpWorker, transport: UcxShuffleTransport)
  extends Thread with Logging {
  private val taskQueue = new ConcurrentLinkedQueue[Runnable]
  private val waiting = new AtomicBoolean(false)
  private val running = new AtomicBoolean(true)

  setDaemon(true)
  setName("Global worker progress thread")

  private val replyWorkersThreadPool = ThreadUtils.newDaemonFixedThreadPool(
    transport.ucxShuffleConf.numListenerThreads, "UcxListenerThread")

  private val fetchThreadPool = ThreadUtils.newDaemonFixedThreadPool(
    transport.ucxShuffleConf.numWorkers, "UcxFetcherThread")

  @inline
  def submitServer(task: Runnable): Unit = {
    replyWorkersThreadPool.submit(task)
  }

  @inline
  def submitClient(task: Runnable): Unit = {
    fetchThreadPool.submit(task)
  }

  @inline
  def post(task: Runnable): Unit = {
    taskQueue.add(task)
    if (waiting.compareAndSet(true, false)) {
      globalWorker.signal()
    }
  }

  def close(): Unit = {
    running.set(false)
    globalWorker.signal()
    globalWorker.close()
  }

  override def run(): Unit = {
    val useWakeup = transport.ucxShuffleConf.useWakeup
    while (running.get()) {
      val task = taskQueue.poll()
      if (task != null) {
        task.run()
      }
      while (globalWorker.progress != 0) {}
      if (useWakeup && waiting.compareAndSet(false, true) && taskQueue.isEmpty) {
        globalWorker.waitForEvents()
      }
    }
  }
}
