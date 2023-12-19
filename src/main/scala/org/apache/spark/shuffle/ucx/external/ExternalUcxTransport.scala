/*
 * Copyright (C) 2022, NVIDIA CORPORATION & AFFILIATES. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx

import org.apache.spark.shuffle.ucx.memory.UcxHostBounceBuffersPool
import org.apache.spark.shuffle.utils.UcxLogging
import org.openucx.jucx.ucp._

import java.nio.ByteBuffer
import java.util.concurrent.{Executors, ExecutorService}

class ExternalUcxTransport(var ucxShuffleConf: ExternalUcxConf) extends UcxLogging {
  @volatile protected var initialized: Boolean = false
  @volatile protected var running: Boolean = true
  private[ucx] var ucxContext: UcpContext = _
  private[ucx] var hostBounceBufferMemoryPool: UcxHostBounceBuffersPool = _
  private[ucx] val ucpWorkerParams = new UcpWorkerParams().requestThreadSafety()
  private[ucx] var progressExecutors: ExecutorService = _

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
    hostBounceBufferMemoryPool = new UcxHostBounceBuffersPool(ucxContext)
  }

  def initProgressPool(threadNum: Int): Unit = {
    progressExecutors = Executors.newFixedThreadPool(threadNum)
  }

  def init(): ByteBuffer = ???

  def close(): Unit = {
    if (initialized) {
      running = false
      if (hostBounceBufferMemoryPool != null) {
        hostBounceBufferMemoryPool.close()
      }
      if (ucxContext != null) {
        ucxContext.close()
      }
      if (progressExecutors != null) {
        progressExecutors.shutdown()
      }
      initialized = false
    }
  }
}

private[ucx] class UcxStreamState(val callback: OperationCallback,
                                  val request: UcxRequest,
                                  var remaining: Int) {}

private[ucx] class UcxSliceState(val callback: OperationCallback,
                                 val request: UcxRequest,
                                 val mem: MemoryBlock,
                                 var offset: Long,
                                 var remaining: Int) {}

private[ucx] class ProgressThread(
  name: String, worker: UcpWorker, useWakeup: Boolean) extends Thread {
  setDaemon(true)
  setName(name)

  override def run(): Unit = {
    while (!isInterrupted) {
      worker.synchronized {
        while (worker.progress != 0) {}
      }
      if (useWakeup) {
        worker.waitForEvents()
      }
    }
  }
}