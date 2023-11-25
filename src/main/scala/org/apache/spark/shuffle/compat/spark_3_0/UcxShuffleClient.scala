/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.compat.spark_3_0

import java.nio.ByteBuffer
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, BlockStoreClient, DownloadFileManager}
import org.apache.spark.shuffle.ucx.{OperationCallback, OperationResult, UcxShuffleBlockId, UcxShuffleTransport}
import org.apache.spark.shuffle.utils.{UnsafeUtils, CommonUtils}
import org.apache.spark.storage.{BlockId => SparkBlockId, ShuffleBlockId => SparkShuffleBlockId}
import org.apache.spark.SparkException

class UcxShuffleClient(val transport: UcxShuffleTransport, mapId2PartitionId: Map[Long, Int]) extends BlockStoreClient with Logging {

  override def fetchBlocks(host: String, port: Int, execId: String, blockIds: Array[String],
                           listener: BlockFetchingListener,
                           downloadFileManager: DownloadFileManager): Unit = {
    val ucxBlockIds = Array.ofDim[UcxShuffleBlockId](blockIds.length)
    val callbacks = Array.ofDim[OperationCallback](blockIds.length)
    var send = 0
    var receive = 0
    for (i <- blockIds.indices) {
      send = send + 1
      SparkBlockId.apply(blockIds(i)) match {
        case blockId: SparkShuffleBlockId => {
          ucxBlockIds(i) = UcxShuffleBlockId(blockId.shuffleId, mapId2PartitionId(blockId.mapId), blockId.reduceId)
          callbacks(i) = (result: OperationResult) => {
            val memBlock = result.getData
            val buffer = UnsafeUtils.getByteBufferView(memBlock.address, memBlock.size.toInt)
            listener.onBlockFetchSuccess(blockIds(i), new NioManagedBuffer(buffer) {
              override def release: ManagedBuffer = {
                memBlock.close()
                this
              }
            })
          }
          val resultBufferAllocator = (size: Long) => transport.hostBounceBufferMemoryPool.get(size)
          transport.fetchBlocksByBlockIds(execId.toLong, Array(ucxBlockIds(i)), resultBufferAllocator, 
            Array(callbacks(i)), () => {receive = receive + 1})
        }
        case _ =>
          throw new SparkException("Unrecognized blockId")
      }
    }

    val resultBufferAllocator = (size: Long) => transport.hostBounceBufferMemoryPool.get(size)
    transport.fetchBlocksByBlockIds(execId.toLong, ucxBlockIds, resultBufferAllocator, callbacks, () => {})
  }

  override def close(): Unit = {

  }
}
