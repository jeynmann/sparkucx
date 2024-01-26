package org.apache.spark.shuffle.ucx

// import org.apache.spark.SparkEnv
import org.apache.spark.shuffle.utils.{UcxLogging, UnsafeUtils}
import org.apache.spark.shuffle.ucx.utils.SerializationUtils
import org.apache.spark.network.buffer.FileSegmentManagedBuffer
import org.apache.spark.network.shuffle.ExternalUcxShuffleBlockResolver
import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants
import org.openucx.jucx.ucs.UcsConstants.MEMORY_TYPE
import org.openucx.jucx.{UcxCallback, UcxException, UcxUtils}

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import scala.collection.concurrent.TrieMap

class ExternalUcxServerTransport(
  serverConf: ExternalUcxServerConf, blockManager: ExternalUcxShuffleBlockResolver)
  extends ExternalUcxTransport(serverConf)
  with UcxLogging {
  private[ucx] val workerMap = new TrieMap[String, TrieMap[UcxWorkerId, Unit]]
  private[ucx] val fileMap = new TrieMap[String, ConcurrentHashMap[UcxShuffleMapId, FileChannel]]

  private[ucx] var allocatedWorker: Array[ExternalUcxServerWorker] = _
  private[ucx] var globalWorker: ExternalUcxServerWorker = _
  private[ucx] var serverPorts: Seq[Int] = _
  private var serverPortsBuffer: ByteBuffer = _

  override def estimateNumEps(): Int = serverConf.ucxEpsNum

  override def init(): ByteBuffer = {
    initContext()
    initMemoryPool()

    if (serverConf.useWakeup) {
      ucpWorkerParams.requestWakeupRX().requestWakeupTX().requestWakeupEdge()
    }

    initTaskPool(serverConf.numThreads)

    logInfo(s"Allocating ${serverConf.numWorkers} server workers")

    allocatedWorker = new Array[ExternalUcxServerWorker](serverConf.numWorkers)
    for (i <- 0 until serverConf.numWorkers) {
      val worker = ucxContext.newWorker(ucpWorkerParams)
      val workerId = new UcxWorkerId("Server", 0, i)
      allocatedWorker(i) = new ExternalUcxServerWorker(worker, this, workerId, 0)
    }
    serverPorts = allocatedWorker.map(_.getPort)

    serverPortsBuffer = ByteBuffer.allocateDirect(
      serverPorts.length * UnsafeUtils.INT_SIZE)
    serverPorts.foreach(serverPortsBuffer.putInt(_))
    serverPortsBuffer.rewind()

    logInfo(s"Launching ${serverConf.numWorkers} server workers")
    allocatedWorker.foreach(_.start)

    logInfo(s"Allocating global worker")

    val worker = ucxContext.newWorker(ucpWorkerParams)
    globalWorker = new ExternalUcxServerWorker(
      worker, this, new UcxWorkerId("Listener", 0, 0), serverConf.ucxServerPort)
    globalWorker.start

    initialized = true
    logInfo(s"Started listener on ${globalWorker.getAddress} ${serverPorts}")
    SerializationUtils.serializeInetAddress(globalWorker.getAddress)
  }

  /**
   * Close all transport resources
   */
  override def close(): Unit = {
    if (initialized) {
      running = false

      if (globalWorker != null) {
        globalWorker.closing().get(1, TimeUnit.MILLISECONDS)
      }

      if (allocatedWorker != null) {
        allocatedWorker.map(_.closing).foreach(_.get(5, TimeUnit.MILLISECONDS))
      }

      super.close()

      logInfo("UCX transport closed.")
    }
  }

  def applicationRemoved(appId: String): Unit = {
    workerMap.remove(appId).foreach(clients => {
      val clientIds = clients.keys.toSeq
      allocatedWorker.foreach(_.disconnect(clientIds))
      globalWorker.disconnect(clientIds)
    })
    fileMap.remove(appId).foreach(files => files.values.forEach(_.close))
    // allocatedWorker.foreach(_.debugClients())
  }

  def executorRemoved(executorId: String, appId: String): Unit = {
    val exeId = executorId.toInt
    workerMap.get(appId).map(clients => {
      val clientIds = clients.filterKeys(_.exeId == exeId).keys.toSeq
      allocatedWorker.foreach(_.disconnect(clientIds))
      globalWorker.disconnect(clientIds)
    })
  }

  def getServerPortsBuffer(): ByteBuffer = {
    serverPortsBuffer.duplicate()
  }

  def handleConnect(handler: ExternalUcxServerWorker,
                    clientWorker: UcxWorkerId, address: ByteBuffer): Unit = {
    workerMap.getOrElseUpdate(clientWorker.appId, {
      new TrieMap[UcxWorkerId, Unit]
    }).getOrElseUpdate(clientWorker, Unit)
  }

  def handleFetchBlockRequest(handler: ExternalUcxServerWorker,
                              clientWorker: UcxWorkerId, exeId: Int,
                              replyTag: Int, blockIds: Seq[UcxShuffleBlockId]):
                              Unit = {
    submit(new Runnable {
      override def run(): Unit = {
        val blockInfos = blockIds.map(bid => {
          val block = blockManager.getBlockData(clientWorker.appId, exeId.toString,
                                                bid.shuffleId, bid.mapId,
                                                bid.reduceId).asInstanceOf[
                                                  FileSegmentManagedBuffer]
          (openBlock(clientWorker.appId, bid, block), block.getOffset, block.size)
        })
        handler.handleFetchBlockRequest(clientWorker, replyTag, blockInfos)
      }
    })
  }

  def handleFetchBlockStream(handler: ExternalUcxServerWorker,
                             clientWorker: UcxWorkerId, exeId: Int,
                             replyTag: Int, bid: UcxShuffleBlockId): Unit = {
    submit(new Runnable {
      override def run(): Unit = {
        val block = blockManager.getBlockData(clientWorker.appId, exeId.toString,
                                              bid.shuffleId, bid.mapId,
                                              bid.reduceId).asInstanceOf[
                                                FileSegmentManagedBuffer]
        val blockInfo = (
          openBlock(clientWorker.appId, bid, block), block.getOffset, block.size)
        handler.handleFetchBlockStream(clientWorker, replyTag, blockInfo)
      }
    })
  }

  def openBlock(appId: String, bid: UcxShuffleBlockId,
                blockData: FileSegmentManagedBuffer): FileChannel = {
    fileMap.getOrElseUpdate(appId, {
      new ConcurrentHashMap[UcxShuffleMapId, FileChannel]
    }).computeIfAbsent(
      UcxShuffleMapId(bid.shuffleId, bid.mapId),
      _ => FileChannel.open(blockData.getFile().toPath(), StandardOpenOption.READ)
    )
  }
}
