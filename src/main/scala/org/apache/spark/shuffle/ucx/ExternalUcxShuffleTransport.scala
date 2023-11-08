/*
 * Copyright (C) 2022, NVIDIA CORPORATION & AFFILIATES. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx

// import org.apache.spark.SparkEnv
import org.apache.spark.shuffle.ucx.memory.UcxHostBounceBuffersPool
import org.apache.spark.shuffle.utils.UcxLogging
// import org.apache.spark.util.ThreadUtils
import org.openucx.jucx.ucp._

import java.nio.ByteBuffer
import java.util.concurrent.{Executors, ExecutorService}
import java.util.concurrent.atomic.AtomicInteger

case class UcxWorkerId(appId: String, exeId: Int, workerId: Int) extends BlockId {
  override def serializedSize: Int = 12 + appId.size

  override def serialize(byteBuffer: ByteBuffer): Unit = {
    byteBuffer.putInt(exeId)
    byteBuffer.putInt(workerId)
    byteBuffer.putInt(appId.size)
    byteBuffer.put(appId.getBytes)
  }

  override def toString(): String = s"UcxWorkerId($appId, $exeId, $workerId)"
}

object UcxWorkerId {
  def deserialize(byteBuffer: ByteBuffer): UcxWorkerId = {
    val exeId = byteBuffer.getInt
    val workerId = byteBuffer.getInt
    val appIdSize = byteBuffer.getInt
    val appIdBytes = new Array[Byte](appIdSize)
    byteBuffer.get(appIdBytes)
    UcxWorkerId(new String(appIdBytes), exeId, workerId)
  }
}

class ExternalShuffleTransport(var ucxShuffleConf: ExternalUcxConf) extends UcxLogging {
  @volatile protected var initialized: Boolean = false
  @volatile protected var running: Boolean = true
  private[ucx] var ucxContext: UcpContext = _
  private[ucx] var hostBounceBufferMemoryPool: UcxHostBounceBuffersPool = _
  private[ucx] val ucpWorkerParams = new UcpWorkerParams().requestThreadSafety()
  private[ucx] var allocatedWorker: Array[ExternalUcxWorkerWrapper] = _
  private[ucx] val currentWorkerId = new AtomicInteger()
  private[ucx] var progressExecutors: ExecutorService = _

  class ProgressTask(worker: UcpWorker) extends Runnable {
    override def run(): Unit = {
      val useWakeup = ucxShuffleConf.useWakeup
      while (running) {
        worker.synchronized {
          while (worker.progress != 0) {}
        }
        if (useWakeup) {
          worker.waitForEvents()
        }
      }
    }
  }

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

  def initWorker(workerNum: Int, creator: (Int) => ExternalUcxWorkerWrapper): Unit = {
    allocatedWorker = new Array[ExternalUcxWorkerWrapper](workerNum)
    for (i <- 0 until workerNum) {
      allocatedWorker(i) = creator(i)
      progressExecutors.execute(new ProgressTask(allocatedWorker(i).worker))
    }
  }

  def init(): ByteBuffer = ???

  @inline
  def selectWorker(): ExternalUcxWorkerWrapper = allocatedWorker(
    (currentWorkerId.incrementAndGet() % allocatedWorker.length).abs)

  def close(): Unit = {
    if (initialized) {
      running = false
      if (hostBounceBufferMemoryPool != null) {
        hostBounceBufferMemoryPool.close()
      }
      if (ucxContext != null) {
        ucxContext.close()
      }
      if (allocatedWorker != null) {
        allocatedWorker.foreach(_.close)
      }
      if (progressExecutors != null) {
        progressExecutors.shutdown()
      }
      initialized = false
    }
  }
}
