/*
 * Copyright (C) 2022, NVIDIA CORPORATION & AFFILIATES. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx

// import org.apache.spark.SparkEnv
import org.apache.spark.shuffle.ucx.memory.UcxHostBounceBuffersPool
import org.apache.spark.shuffle.ucx.rpc.GlobalWorkerRpcThread
import org.apache.spark.shuffle.ucx.utils.{SerializableDirectBuffer, SerializationUtils}
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.network.shuffle.UcxLogging
import org.apache.spark.network.shuffle.ExternalUcxShuffleBlockResolver
import org.apache.spark.storage.BlockManagerId
import org.openucx.jucx.UcxException
import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

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
  var ucxContext: UcpContext = _
  var hostBounceBufferMemoryPool: UcxHostBounceBuffersPool = _
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

  def init(): ByteBuffer = ???

  def close(): Unit = {
    if (initialized) {
      hostBounceBufferMemoryPool.close()
      ucxContext.close()
      initialized = false
    }
  }
}
