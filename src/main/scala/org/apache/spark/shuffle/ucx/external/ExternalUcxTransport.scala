/*
 * Copyright (C) 2022, NVIDIA CORPORATION & AFFILIATES. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx

import org.apache.spark.shuffle.ucx.memory.UcxLimitedMemPool
import org.apache.spark.shuffle.utils.UcxLogging
import org.openucx.jucx.ucp._

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

class ExternalUcxTransport(val ucxShuffleConf: ExternalUcxConf) extends UcxLogging {
  @volatile protected var initialized: Boolean = false
  protected var ucxContext: UcpContext = _
  protected val ucpWorkerParams = new UcpWorkerParams().requestThreadSafety()
  private[ucx] var hostBounceBufferMemoryPool: UcxLimitedMemPool = _
  private[ucx] var worker: UcpWorker = _
  private[ucx] var progressThread: ProgressThread = _

  def estimateNumEps(): Int = 1

  def initContext(): Unit = {
    val numEndpoints = estimateNumEps()
    logInfo(s"Creating UCX context with an estimated number of endpoints: $numEndpoints")

    val params = new UcpParams().requestAmFeature().setMtWorkersShared(true)
      .setEstimatedNumEps(numEndpoints).requestAmFeature()
      .setConfig("USE_MT_MUTEX", "yes")

    if (ucxShuffleConf.useWakeup) {
      params.requestWakeupFeature()
    }

    ucxContext = new UcpContext(params)
  }

  def initMemoryPool(): Unit = {
    hostBounceBufferMemoryPool = new UcxLimitedMemPool(ucxContext)
    hostBounceBufferMemoryPool.init(ucxShuffleConf.minBufferSize,
                                    ucxShuffleConf.maxBufferSize,
                                    ucxShuffleConf.minRegistrationSize,
                                    ucxShuffleConf.maxRegistrationSize,
                                    ucxShuffleConf.preallocateBuffersMap,
                                    ucxShuffleConf.memoryLimit)
  }

  def initWorker(): Unit = {
    if (ucxShuffleConf.useWakeup) {
      ucpWorkerParams.requestWakeupRX().requestWakeupTX().requestWakeupEdge()
    }

    logInfo(s"Allocating global workers")
    worker = ucxContext.newWorker(ucpWorkerParams)
  }

  def initProgressPool(threadNum: Int, useWakeup: Boolean): Unit = {
    progressThread = new ProgressThread(worker, threadNum, useWakeup)
    progressThread.start()
  }

  def init(): ByteBuffer = ???

  @inline
  def submit(task: Runnable): Unit = {
    progressThread.submit(task)
  }

  @inline
  def post(task: Runnable): Unit = {
    progressThread.post(task)
  }

  def close(): Unit = {
    if (initialized) {
      if (hostBounceBufferMemoryPool != null) {
        hostBounceBufferMemoryPool.close()
        hostBounceBufferMemoryPool = null
      }
      if (progressThread != null) {
        progressThread.close()
        progressThread = null
      }
      if (ucxContext != null) {
        ucxContext.close()
        ucxContext = null
      }
      initialized = false
    }
  }
}

class ProgressThread(worker: UcpWorker, threadNum: Int, useWakeup: Boolean)
  extends Thread with UcxLogging {
  private val taskQueue = new ConcurrentLinkedQueue[Runnable]
  private val waiting = new AtomicBoolean(false)
  private val running = new AtomicBoolean(true)

  setDaemon(true)
  setName("UCX-progress")

  private val replyWorkersThreadPool = UcxThreadUtils.newFixedDaemonPool(
    "UcxListenerThread", threadNum)

  @inline
  def submit(task: Runnable): Unit = {
    replyWorkersThreadPool.submit(task)
  }

  @inline
  def post(task: Runnable): Unit = {
    taskQueue.add(task)
    if (waiting.compareAndSet(true, false)) {
      worker.signal()
    }
  }

  def close(): Unit = {
    running.set(false)
    worker.signal()
    worker.close()
  }

  override def run(): Unit = {
    while (running.get()) {
      val task = taskQueue.poll()
      if (task != null) {
        task.run()
      }
      while (worker.progress != 0) {}
      if (useWakeup && waiting.compareAndSet(false, true) && taskQueue.isEmpty) {
        worker.waitForEvents()
      }
    }
  }
}
