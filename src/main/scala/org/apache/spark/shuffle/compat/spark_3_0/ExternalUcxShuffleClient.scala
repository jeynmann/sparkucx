package org.apache.spark.shuffle.compat.spark_2_4

import org.openucx.jucx.UcxUtils
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFileManager, BlockStoreClient}
import org.apache.spark.shuffle.ucx.{OperationCallback, OperationResult, UcxShuffleBlockId, UcxShuffleTransportClient, UcxFetchCallBack, UcxDownloadCallBack}
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.storage.{BlockId => SparkBlockId, ShuffleBlockId => SparkShuffleBlockId}

class ExternalUcxShuffleClient(val transport: UcxShuffleTransportClient) extends BlockStoreClient{

  override def fetchBlocks(host: String, port: Int, execId: String, blockIds: Array[String],
                           listener: BlockFetchingListener,
                           downloadFileManager: DownloadFileManager): Unit = {
    if (downloadFileManager == null) {
      val ucxBlockIds = Array.ofDim[UcxShuffleBlockId](blockIds.length)
      val callbacks = Array.ofDim[OperationCallback](blockIds.length)
      for (i <- blockIds.indices) {
        val blockId = SparkBlockId.apply(blockIds(i))
                                  .asInstanceOf[SparkShuffleBlockId]
        ucxBlockIds(i) = UcxShuffleBlockId(blockId.shuffleId, blockId.mapId,
                                           blockId.reduceId)
        callbacks(i) = new UcxFetchCallBack(blockIds(i), listener)
      }
      transport.fetchBlocksByBlockIds(host, execId.toInt, ucxBlockIds, callbacks)
    } else {
      for (i <- blockIds.indices) {
        val blockId = SparkBlockId.apply(blockIds(i))
                                  .asInstanceOf[SparkShuffleBlockId]
        val ucxBid = UcxShuffleBlockId(blockId.shuffleId, blockId.mapId,
                                       blockId.reduceId)
        val callback = new UcxDownloadCallBack(blockIds(i), listener,
                                               downloadFileManager,
                                               transport.sparkTransportConf)
        transport.fetchBlockByStream(host, execId.toInt, ucxBid, callback)
      }
    }
  }

  override def close(): Unit = {

  }
}
