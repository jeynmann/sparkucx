package org.apache.spark.shuffle.compat.spark_2_4

import org.openucx.jucx.UcxUtils
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFileManager, ShuffleClient}
import org.apache.spark.shuffle.ucx.{OperationCallback, OperationResult, UcxShuffleBockId, UcxShuffleTransport}
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.storage.{BlockId => SparkBlockId, ShuffleBlockId => SparkShuffleBlockId}

class UcxShuffleClient(val transport: UcxShuffleTransport) extends ShuffleClient{
  val worker = transport.selectLocalWorker()
  override def fetchBlocks(host: String, port: Int, execId: String, blockIds: Array[String],
                           listener: BlockFetchingListener,
                           downloadFileManager: DownloadFileManager): Unit = {
    val ucxBlockIds = Array.ofDim[UcxShuffleBockId](blockIds.length)
    val callbacks = Array.ofDim[OperationCallback](blockIds.length)
    for (i <- blockIds.indices) {
      val blockId = SparkBlockId.apply(blockIds(i)).asInstanceOf[SparkShuffleBlockId]
      ucxBlockIds(i) = UcxShuffleBockId(blockId.shuffleId, blockId.mapId, blockId.reduceId)
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
    }
    val resultBufferAllocator = (size: Long) => transport.hostBounceBufferMemoryPool.get(size)
    worker.fetchBlocksByBlockIds(execId.toLong, ucxBlockIds, resultBufferAllocator, callbacks)
  }

  override def close(): Unit = {
    transport.releaseLocalWorker()
  }

  def progress(): Unit = {
    worker.progress()
  }
}
