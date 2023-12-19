package org.apache.spark.shuffle.ucx

import java.nio.ByteBuffer
import scala.concurrent.forkjoin.{ForkJoinPool => SForkJoinPool, ForkJoinWorkerThread => SForkJoinWorkerThread}

case class UcxShuffleBlockId(shuffleId: Int, mapId: Int, reduceId: Int) extends BlockId {
  override def serializedSize: Int = 12

  override def serialize(byteBuffer: ByteBuffer): Unit = {
    byteBuffer.putInt(shuffleId)
    byteBuffer.putInt(reduceId)
    byteBuffer.putInt(mapId)
  }
}

object UcxShuffleBlockId {
  def deserialize(byteBuffer: ByteBuffer): UcxShuffleBlockId = {
    val shuffleId = byteBuffer.getInt
    val reduceId = byteBuffer.getInt
    val mapId = byteBuffer.getInt
    UcxShuffleBlockId(shuffleId, mapId, reduceId)
  }
}

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

  @`inline`
  def makeExeWorkerId(id: UcxWorkerId): Long = {
    (id.workerId.toLong << 32) | id.exeId
  }

  @`inline`
  def extractExeId(exeWorkerId: Long): Int = {
    exeWorkerId.toInt
  }

  @`inline`
  def extractWorkerId(exeWorkerId: Long): Int = {
    (exeWorkerId >> 32).toInt
  }

  def apply(appId: String, exeWorkerId: Long): UcxWorkerId = {
    UcxWorkerId(appId, UcxWorkerId.extractExeId(exeWorkerId),
                UcxWorkerId.extractWorkerId(exeWorkerId))
  }
}

object UcxThreadUtils {
  def newForkJoinPool(prefix: String, maxThreadNumber: Int): SForkJoinPool = {
    val factory = new SForkJoinPool.ForkJoinWorkerThreadFactory {
      override def newThread(pool: SForkJoinPool) =
        new SForkJoinWorkerThread(pool) {
          setName(s"${prefix}-${super.getName}")
        }
    }
    new SForkJoinPool(maxThreadNumber, factory, null, false)
  }
}