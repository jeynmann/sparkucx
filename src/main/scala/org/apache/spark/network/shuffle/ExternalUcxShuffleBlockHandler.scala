package org.apache.spark.network.shuffle

import java.io.File
import java.io.IOException
import java.nio.ByteBuffer
import java.util.HashMap
import java.util.Iterator
import java.util.Map

import com.codahale.metrics.Gauge
import com.codahale.metrics.Meter
import com.codahale.metrics.Metric
import com.codahale.metrics.MetricSet
import com.codahale.metrics.Timer
import com.google.common.annotations.VisibleForTesting
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.RpcResponseCallback
import org.apache.spark.network.client.TransportClient
import org.apache.spark.network.server.OneForOneStreamManager
import org.apache.spark.network.server.RpcHandler
import org.apache.spark.network.server.StreamManager
import org.apache.spark.network.shuffle.ExternalShuffleBlockResolver.AppExecId
import org.apache.spark.network.shuffle.protocol._
import org.apache.spark.network.util.NettyUtils
import org.apache.spark.network.util.TransportConf

class ExternalUcxShuffleBlockHandler(conf: TransportConf, registeredExecutorFile: File) extends RpcHandler with UcxLogging{
  val blockManager = new ExternalUcxShuffleBlockResolver(conf, registeredExecutorFile)
  val streamManager = new OneForOneStreamManager()
  val metrics = new ShuffleMetrics()

  def receive(client: TransportClient, message: ByteBuffer, callback: RpcResponseCallback): Unit = {
    val msgObj = BlockTransferMessage.Decoder.fromByteBuffer(message)
    handleMessage(msgObj, client, callback)
  }

  def handleMessage(
    msgObj: BlockTransferMessage,
    client: TransportClient,
    callback: RpcResponseCallback): Unit = {
    if (msgObj.isInstanceOf[OpenBlocks]) {
      val responseDelayContext = metrics.openBlockRequestLatencyMillis.time()
      try {
        val msg = msgObj.asInstanceOf[OpenBlocks]
        checkAuth(client, msg.appId)
        val streamId = streamManager.registerStream(client.getClientId(),
          new ManagedBufferIterator(msg.appId, msg.execId, msg.blockIds)) // @C , client.getChannel()
        logTrace(s"Registered streamId ${streamId} with ${msg.blockIds.length} buffers for client ${client.getClientId()} from host ${NettyUtils.getRemoteAddress(client.getChannel())}")
        callback.onSuccess(new StreamHandle(streamId, msg.blockIds.length).toByteBuffer())
      } finally {
        responseDelayContext.stop()
      }
    } else if (msgObj.isInstanceOf[RegisterExecutor]) {
      val responseDelayContext = metrics.registerExecutorRequestLatencyMillis.time()
      try {
        val msg = msgObj.asInstanceOf[RegisterExecutor]
        checkAuth(client, msg.appId)
        blockManager.registerExecutor(msg.appId, msg.execId, msg.executorInfo)
        callback.onSuccess(ByteBuffer.wrap(new Array[Byte](0)))
      } finally {
        responseDelayContext.stop()
      }
    } else {
      throw new UnsupportedOperationException("Unexpected message: " + msgObj)
    }
  }

  def getAllMetrics(): MetricSet = metrics

  def getStreamManager(): StreamManager = streamManager

  /**
   * Removes an application (once it has been terminated), and optionally will clean up any
   * local directories associated with the executors of that application in a separate thread.
   */
  def applicationRemoved(appId: String, cleanupLocalDirs: Boolean): Unit = {
    blockManager.applicationRemoved(appId, cleanupLocalDirs)
  }

  /**
   * Clean up any non-shuffle files in any local directories associated with an finished executor.
   */
  def executorRemoved(executorId: String, appId: String): Unit = {
    blockManager.executorRemoved(executorId, appId)
  }

  /**
   * Register an (application, executor) with the given shuffle info.
   *
   * The "re-" is meant to highlight the intended use of this method -- when this service is
   * restarted, this is used to restore the state of executors from before the restart.  Normal
   * registration will happen via a message handled in receive()
   *
   * @param appExecId
   * @param executorInfo
   */
  def reregisterExecutor(appExecId: AppExecId, executorInfo: ExecutorShuffleInfo): Unit = {
    blockManager.registerExecutor(appExecId.appId, appExecId.execId, executorInfo)
  }

  def close(): Unit = {
    blockManager.close()
  }

  def checkAuth(client: TransportClient, appId: String): Unit = {
    if (client.getClientId() != null && !client.getClientId().equals(appId)) {
      throw new SecurityException(String.format(
        "Client for %s not authorized for application %s.", client.getClientId(), appId))
    }
  }

  /**
   * A simple class to wrap all shuffle service wrapper metrics
   */
  class ShuffleMetrics extends MetricSet {
    val allMetrics = new HashMap[String, Metric]()
    // Time latency for open block request in ms
    val openBlockRequestLatencyMillis = new Timer()
    // Time latency for executor registration latency in ms
    val registerExecutorRequestLatencyMillis = new Timer()
    // Block transfer rate in byte per second
    val blockTransferRateBytes = new Meter()

    init()

    def init(): Unit = {
      allMetrics.put("openBlockRequestLatencyMillis", openBlockRequestLatencyMillis)
      allMetrics.put("registerExecutorRequestLatencyMillis", registerExecutorRequestLatencyMillis)
      allMetrics.put("blockTransferRateBytes", blockTransferRateBytes)
      allMetrics.put("registeredExecutorsSize", new Gauge[Integer] {
        override def getValue() = blockManager.getRegisteredExecutorsSize()
      })
    }

    override def getMetrics() = allMetrics
  }

  class ManagedBufferIterator(appId: String, execId: String, blockIds: Array[String]) extends Iterator[ManagedBuffer] {
    var index = 0
    var shuffleId = 0
    // An array containing mapId and reduceId pairs.
    val mapIdAndReduceIds = new Array[Int](2 * blockIds.length)

    def init(): Unit = {
      val blockId0Parts = blockIds(0).split("_")
      if (blockId0Parts.length != 4 || !blockId0Parts(0).equals("shuffle")) {
        throw new IllegalArgumentException("Unexpected shuffle block id format: " + blockIds(0))
      }
      shuffleId = Integer.parseInt(blockId0Parts(1))
      for (i <- 0 until blockIds.length) {
        val blockIdParts = blockIds(i).split("_")
        if (blockIdParts.length != 4 || !blockIdParts(0).equals("shuffle")) {
          throw new IllegalArgumentException("Unexpected shuffle block id format: " + blockIds(i))
        }
        if (Integer.parseInt(blockIdParts(1)) != shuffleId) {
          throw new IllegalArgumentException("Expected shuffleId=" + shuffleId +
            ", got:" + blockIds(i))
        }
        mapIdAndReduceIds(2 * i) = Integer.parseInt(blockIdParts(2))
        mapIdAndReduceIds(2 * i + 1) = Integer.parseInt(blockIdParts(3))
      }
    }

    override def hasNext() = {
      index < mapIdAndReduceIds.length
    }

    override def next() = {
      val block = blockManager.getBlockData(appId, execId, shuffleId,
        mapIdAndReduceIds(index), mapIdAndReduceIds(index + 1))
      index += 2
      metrics.blockTransferRateBytes.mark(if (block != null) block.size() else 0)
      block
    }
  }
}