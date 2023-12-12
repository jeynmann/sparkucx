/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.compat.spark_3_0

import org.apache.spark.internal.Logging
import org.apache.spark.network.util.TransportConf
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, BlockStoreClient, DownloadFileManager}
import org.apache.spark.shuffle.ucx.{OperationCallback, OperationResult, UcxShuffleBockId, UcxShuffleTransport}
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.storage.{BlockId => SparkBlockId, ShuffleBlockId => SparkShuffleBlockId}
import java.nio.ByteBuffer

class UcxShuffleClient(val transport: UcxShuffleTransport, mapId2PartitionId: Map[Long, Int]) extends BlockStoreClient with Logging {
  private lazy val maxBlocksPerRequest = transport.ucxShuffleConf.maxBlocksPerRequest

  override def fetchBlocks(host: String, port: Int, execId: String, blockIds: Array[String],
                           listener: BlockFetchingListener,
                           downloadFileManager: DownloadFileManager): Unit = {
    val resultBufferAllocator =
      (size: Long) => transport.hostBounceBufferMemoryPool.get(size)
    if (downloadFileManager == null) {
      val ucxBlockIds = Array.ofDim[UcxShuffleBockId](blockIds.length)
      val callbacks = Array.ofDim[OperationCallback](blockIds.length)
      for (i <- blockIds.indices) {
        val blockId = SparkBlockId.apply(blockIds(i))
                                  .asInstanceOf[SparkShuffleBlockId]
        ucxBlockIds(i) = UcxShuffleBockId(blockId.shuffleId,
                                          mapId2PartitionId(blockId.mapId),
                                          blockId.reduceId)
        callbacks(i) = new FetchCallBack(blockIds(i), listener)
      }
      for (i <- 0 until blockIds.length by maxBlocksPerRequest) {
        val j = i + maxBlocksPerRequest
        transport.fetchBlocksByBlockIds(execId.toLong, ucxBlockIds.slice(i, j),
                                        resultBufferAllocator,
                                        callbacks.slice(i, j))
      }
    } else {
      for (i <- blockIds.indices) {
        val blockId = SparkBlockId.apply(blockIds(i))
                                  .asInstanceOf[SparkShuffleBlockId]
        val ucxBid = UcxShuffleBockId(blockId.shuffleId,
                                      mapId2PartitionId(blockId.mapId),
                                      blockId.reduceId)
        val callback = new DownloadCallBack(blockIds(i), listener,
                                            downloadFileManager,
                                            transport.sparkTransportConf)
        transport.fetchBlockByStream(execId.toLong, ucxBid,
                                     resultBufferAllocator, callback)
      }
    }
  }

  override def close(): Unit = {

  }

  private[this] class FetchCallBack(
    blockId: String, listener: BlockFetchingListener)
    extends OperationCallback {

    override def onComplete(result: OperationResult): Unit = {
      val memBlock = result.getData
      val buffer = UnsafeUtils.getByteBufferView(memBlock.address,
                                                 memBlock.size.toInt)
      listener.onBlockFetchSuccess(blockId, new NioManagedBuffer(buffer) {
        override def release: ManagedBuffer = {
          memBlock.close()
          this
        }
      })
    }
  }

  private[this] class DownloadCallBack(
    blockId: String, listener: BlockFetchingListener,
    downloadFileManager: DownloadFileManager, transportConf: TransportConf)
    extends OperationCallback {

    private[this] val targetFile = downloadFileManager.createTempFile(
      transportConf)
    private[this] val channel = targetFile.openForWriting();

    override def onData(buffer: ByteBuffer): Unit = {
      while (buffer.hasRemaining()) {
        channel.write(buffer);
      }
    }

    override def onComplete(result: OperationResult): Unit = {
      listener.onBlockFetchSuccess(blockId, channel.closeAndRead());
      if (!downloadFileManager.registerTempFileToClean(targetFile)) {
        targetFile.delete();
      }
    }
  }
}
