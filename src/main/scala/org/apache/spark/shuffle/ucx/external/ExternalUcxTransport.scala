/*
 * Copyright (C) 2022, NVIDIA CORPORATION & AFFILIATES. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx

import org.apache.spark.shuffle.ucx.memory.UcxLimitedMemPool
import org.apache.spark.shuffle.utils.UcxLogging
import org.openucx.jucx.ucp._

import java.nio.ByteBuffer
import java.util.concurrent.ExecutorService

object ExternalAmId {
  // client -> server
  final val CONNECT = 1
  final val FETCH_BLOCK = 2
  final val FETCH_STREAM = 3
  // server -> client
  final val REPLY_SLICE = 1
  final val REPLY_BLOCK = 2
  final val REPLY_STREAM = 3
}

class ExternalUcxTransport(val ucxShuffleConf: ExternalUcxConf) extends UcxLogging {
  @volatile protected var initialized: Boolean = false
  @volatile protected var running: Boolean = true
  private[ucx] var ucxContext: UcpContext = _
  private[ucx] var hostBounceBufferMemoryPool: UcxLimitedMemPool = _
  private[ucx] val ucpWorkerParams = new UcpWorkerParams().requestThreadSafety()
  private[ucx] var taskExecutors: ExecutorService = _

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

  def initTaskPool(threadNum: Int): Unit = {
    taskExecutors = UcxThreadUtils.newFixedDaemonPool("UCX", threadNum)
  }

  def init(): ByteBuffer = ???

  @`inline`
  def submit(task: Runnable): Unit = {
    taskExecutors.submit(task)
  }

  def close(): Unit = {
    if (initialized) {
      if (hostBounceBufferMemoryPool != null) {
        hostBounceBufferMemoryPool.close()
      }
      if (ucxContext != null) {
        ucxContext.close()
      }
      if (taskExecutors != null) {
        taskExecutors.shutdown()
      }
      initialized = false
    }
  }
}
