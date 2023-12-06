package org.apache.spark.shuffle.compat.spark_2_4

import org.openucx.jucx.UcxUtils
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFileManager, ShuffleClient}
import org.apache.spark.shuffle.ucx.{OperationCallback, OperationResult, UcxShuffleBockId, UcxShuffleTransport}
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.storage.{BlockId => SparkBlockId, ShuffleBlockId => SparkShuffleBlockId}

class UcxShuffleClient(val transport: UcxShuffleTransport) extends ShuffleClient{

  override def fetchBlocks(host: String, port: Int, execId: String, blockIds: Array[String],
                           listener: BlockFetchingListener,
                           downloadFileManager: DownloadFileManager): Unit = {
    val ucxBlockIds = Array.ofDim[UcxShuffleBockId](blockIds.length)
    val callbacks = Array.ofDim[OperationCallback](blockIds.length)
    for (i <- blockIds.indices) {
      val blockId = SparkBlockId.apply(blockIds(i)).asInstanceOf[SparkShuffleBlockId]
      ucxBlockIds(i) = UcxShuffleBockId(blockId.shuffleId, blockId.mapId, blockId.reduceId)
      if (downloadFileManager == null) {
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
      } else {
        callbacks(i) = new DownloadCallBack(blockIds(i), listener, downloadFileManager)
      }
    }
    val resultBufferAllocator = (size: Long) => transport.hostBounceBufferMemoryPool.get(size)
    transport.fetchBlocksByBlockIds(execId.toLong, ucxBlockIds, resultBufferAllocator, callbacks)
  }

  override def close(): Unit = {

  }

  private[this] class DownloadCallBack(
    blockId: String, listener: BlockFetchingListener, downloadFileManager: DownloadFileManager)
    extends OperationCallback {
    private[this] val targetFile = downloadFileManager.createTempFile(
      transport.sparkTransportConf)
    private[this] val channel = targetFile.openForWriting();
    override def onComplete(result: OperationResult): Unit = {
      val memBlock = result.getData
      val buffer = UnsafeUtils.getByteBufferView(memBlock.address, memBlock.size.toInt)
      while (buffer.hasRemaining()) {
        channel.write(buffer);
      }
      memBlock.close()
      listener.onBlockFetchSuccess(blockId, channel.closeAndRead());
      if (!downloadFileManager.registerTempFileToClean(targetFile)) {
        targetFile.delete();
      }
    }
  }
}
