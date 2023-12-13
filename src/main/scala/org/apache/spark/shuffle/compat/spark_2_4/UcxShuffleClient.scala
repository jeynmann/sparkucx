package org.apache.spark.shuffle.compat.spark_2_4

import org.openucx.jucx.UcxUtils
import org.apache.spark.network.util.TransportConf
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFileManager, ShuffleClient}
import org.apache.spark.shuffle.ucx.{OperationCallback, OperationResult, UcxShuffleBockId, UcxShuffleTransport, UcxFetchCallBack, UcxDownloadCallBack}
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.storage.{BlockId => SparkBlockId, ShuffleBlockId => SparkShuffleBlockId}
import java.nio.ByteBuffer

class UcxShuffleClient(val transport: UcxShuffleTransport) extends ShuffleClient{

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
        ucxBlockIds(i) = UcxShuffleBockId(blockId.shuffleId, blockId.mapId,
                                          blockId.reduceId)
        callbacks(i) = new UcxFetchCallBack(blockIds(i), listener)
      }
      transport.fetchBlocksByBlockIds(execId.toLong, ucxBlockIds,
                                      resultBufferAllocator, callbacks)
    } else {
      for (i <- blockIds.indices) {
        val blockId = SparkBlockId.apply(blockIds(i))
                                  .asInstanceOf[SparkShuffleBlockId]
        val ucxBid = UcxShuffleBockId(blockId.shuffleId, blockId.mapId,
                                      blockId.reduceId)
        val callback = new UcxDownloadCallBack(blockIds(i), listener,
                                               downloadFileManager,
                                               transport.sparkTransportConf)
        transport.fetchBlockByStream(execId.toLong, ucxBid,
                                     resultBufferAllocator, callback)
      }
    }
  }

  override def close(): Unit = {

  }
}
