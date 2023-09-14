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

class UcxShuffleTransportClient(clientConf: ExternalUcxClientConf, blockManagerId: BlockManagerId)
  extends ExternalShuffleTransport(clientConf) with UcxLogging {
  private val ucpWorkerParams = new UcpWorkerParams().requestThreadSafety()

  private var allocatedClientThreads: Array[ExternalUcxWorkerThread] = _
  private var clientThreadId = new AtomicInteger()

  override def estimateNumEps(): Int = clientConf.numWorkers *
      clientConf.sparkConf.getInt("spark.executor.instances", 1)

  override def init(): ByteBuffer = {
    // if (clientConf == null) {
    //   clientConf = new UcxShuffleConf(SparkEnv.get.conf)
    // }

    super.initContext()
    super.initMemoryPool()
  
    if (clientConf.useWakeup) {
      ucpWorkerParams.requestWakeupRX().requestWakeupTX().requestWakeupEdge()
    }

    allocatedClientThreads = new Array[ExternalUcxWorkerThread](clientConf.numWorkers)
    logInfo(s"Allocating ${clientConf.numWorkers} client workers")
    val appId = clientConf.sparkConf.getAppId
    val exeId = blockManagerId.executorId.toLong.toInt
    for (i <- 0 until clientConf.numWorkers) {
      val workerId = new UcxWorkerId(appId, exeId, i)
      ucpWorkerParams.setClientId((workerId.exeId.toLong << 32) | workerId.workerId.toLong)
      val worker = ucxContext.newWorker(ucpWorkerParams)
      allocatedClientThreads(i) = new ExternalUcxWorkerThread(worker, this, isClientWorker = true, workerId)
    }

    logInfo(s"Launch ${clientConf.numWorkers} client workers")
    allocatedClientThreads.foreach(_.start)
    initialized = true

    val shuffleServer = new InetSocketAddress(blockManagerId.host, 3338) // @C
    logInfo(s"Shuffle server ${shuffleServer}")
    SerializationUtils.serializeInetAddress(shuffleServer)
  }

  override def initMemoryPool(): Unit = {
    hostBounceBufferMemoryPool = new UcxHostBounceBuffersPool(ucxContext)
    hostBounceBufferMemoryPool.init(clientConf.minRegistrationSize,
      clientConf.minBufferSize,
      clientConf.preallocateBuffersMap)
  }

  def connect(shuffleServer: SerializableDirectBuffer): Unit = {
    allocatedClientThreads.foreach { t => t.submit(new Runnable {
      override def run(): Unit = {
        val addressBuffer = shuffleServer.value
        val address = SerializationUtils.deserializeInetAddress(addressBuffer)
        logInfo(s"Connect ${t.workerId.workerId} to $address")
        t.workerWrapper.connect(address, addressBuffer)
        t.workerWrapper.progressConnect()
      }})
    }
  }

  def connectAll(shuffleServerSet: Set[SerializableDirectBuffer]): Unit = {
    allocatedClientThreads.foreach { t => t.submit(new Runnable {
      override def run(): Unit = {
        shuffleServerSet.foreach{shuffleServer => {
          val addressBuffer = shuffleServer.value
          val address = SerializationUtils.deserializeInetAddress(addressBuffer)
          logInfo(s"ConnectAll ${t.workerId.workerId} to $address")
          t.workerWrapper.connect(address, addressBuffer)
        }}
        t.workerWrapper.progressConnect()
      }
    })}
  }

  /**
   * Close all transport resources
   */
  override def close(): Unit = {
    if (initialized) {
      allocatedClientThreads.foreach(_.close)
      super.close()
    }
  }

  /**
   * Batch version of [[ fetchBlocksByBlockIds ]].
   */
  def fetchBlocksByBlockIds(shuffleServer: InetSocketAddress, blockIds: Seq[BlockId],
                            callbacks: Seq[OperationCallback]): Unit = {
    val client = selectClientThread
    client.submit(new Runnable {
      override def run(): Unit = {
        logInfo(s"@D Send fetch to $shuffleServer")
        client.workerWrapper.fetchBlocksByBlockIds(shuffleServer, blockIds, callbacks)
      }
    })
  }

  @inline
  def selectClientThread(): ExternalUcxWorkerThread = allocatedClientThreads(
    (clientThreadId.incrementAndGet() % allocatedClientThreads.length).abs)
}

class UcxShuffleTransportServer(
  serverConf: ExternalUcxServerConf, blockManager: ExternalUcxShuffleBlockResolver)
  extends ExternalShuffleTransport(serverConf)
  with UcxLogging {
  private val ucpWorkerParams = new UcpWorkerParams().requestThreadSafety()

  private val errorHandler = new UcpEndpointErrorHandler {
    override def onError(ucpEndpoint: UcpEndpoint, errorCode: Int, errorString: String): Unit = {
      if (errorCode == UcsConstants.STATUS.UCS_ERR_CONNECTION_RESET) {
        logWarning(s"Connection closed on ep: $ucpEndpoint")
      } else {
        logError(s"Ep $ucpEndpoint got an error: $errorString")
      }
      endpoints.remove(ucpEndpoint)
      ucpEndpoint.close()
    }
  }

  private var globalThread: ExternalUcxWorkerThread = _
  private var listener: UcpListener = _
  val endpoints = mutable.Set.empty[UcpEndpoint]

  private var allocatedServerThreads: Array[ExternalUcxWorkerThread] = _
  private val serverThreadId = new AtomicInteger()

  override def estimateNumEps(): Int = serverConf.ucxEpsNum

  override def init(): ByteBuffer = {
    super.initContext()
    super.initMemoryPool()

    if (serverConf.useWakeup) {
      ucpWorkerParams.requestWakeupRX().requestWakeupTX().requestWakeupEdge()
    }

    logInfo(s"Allocating global workers")
    val globalWorker = ucxContext.newWorker(ucpWorkerParams)
    listener = globalWorker.newListener(new UcpListenerParams().setSockAddr(
      new InetSocketAddress("0.0.0.0", serverConf.ucxServicePort))
      .setConnectionHandler((ucpConnectionRequest: UcpConnectionRequest) => {
        endpoints.add(globalWorker.newEndpoint(new UcpEndpointParams().setConnectionRequest(ucpConnectionRequest)
          .setPeerErrorHandlingMode().setErrorHandler(errorHandler)
          .setName(s"Endpoint to ${ucpConnectionRequest.getClientId}")))
      }))
    // Main RPC thread. Submit each RPC request to separate thread and send reply back from separate worker.
    globalWorker.setAmRecvHandler(0,
      (headerAddress: Long, headerSize: Long, amData: UcpAmData, _: UcpEndpoint) => {
      val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
      val workerId = UcxWorkerId.deserialize(header)
      val replyTag = header.getInt
      handleFetchBlockRequest(workerId, replyTag, amData)
      logInfo(s"@D Receive fetch from $workerId")
      UcsConstants.STATUS.UCS_INPROGRESS
    }, UcpConstants.UCP_AM_FLAG_PERSISTENT_DATA | UcpConstants.UCP_AM_FLAG_WHOLE_MSG )
    // AM to get worker address for client worker and connect server workers to it
    globalWorker.setAmRecvHandler(1,
      (headerAddress: Long, headerSize: Long, amData: UcpAmData, _: UcpEndpoint) => {
      val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
      val workerAddress = UnsafeUtils.getByteBufferView(amData.getDataAddress, amData.getLength.toInt)
      val workerId = UcxWorkerId.deserialize(header)
      logInfo(s"@D Receive connect from $workerId")
      connectBack(workerId, workerAddress)
      UcsConstants.STATUS.UCS_OK
    }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG)
    globalThread = new ExternalUcxWorkerThread(globalWorker, this, false)

    allocatedServerThreads = new Array[ExternalUcxWorkerThread](serverConf.numListenerThreads)
    logInfo(s"Allocating ${serverConf.numListenerThreads} server workers")
    for (i <- 0 until serverConf.numListenerThreads) {
      val worker = ucxContext.newWorker(ucpWorkerParams)
      allocatedServerThreads(i) = new ExternalUcxWorkerThread(worker, this, false)
    }

    logInfo(s"Launch ${serverConf.numListenerThreads} server workers")
    allocatedServerThreads.foreach(_.start)
    globalThread.start

    initialized = true
    logInfo(s"Started listener on ${listener.getAddress}")
    SerializationUtils.serializeInetAddress(listener.getAddress)
  }

  /**
   * Close all transport resources
   */
  override def close(): Unit = {
    if (initialized) {
      endpoints.foreach(_.closeNonBlockingForce())
      endpoints.clear()

      if (listener != null) {
        listener.close()
        listener = null
      }

      if (globalThread != null) {
        globalThread.close()
        globalThread = null
      }

      allocatedServerThreads.foreach(_.close) 

      super.close()
    }
  }

  def connectBack(clientWorker: UcxWorkerId, workerAddress: ByteBuffer): Unit = {
    val copiedAddress = ByteBuffer.allocateDirect(workerAddress.remaining)
    copiedAddress.put(workerAddress)
    copiedAddress.rewind()
    allocatedServerThreads.foreach(t => t.submit(new Runnable {
      override def run(): Unit = t.workerWrapper.connectBack(clientWorker, copiedAddress)
    }))
  }

  def handleFetchBlockRequest(clientWorker: UcxWorkerId, replyTag: Int, amData: UcpAmData): Unit = {
    val server = selectServerThread
    server.submit(new Runnable {
      override def run(): Unit = {
        val buffer = UnsafeUtils.getByteBufferView(amData.getDataAddress, amData.getLength.toInt)
        val blockIds = mutable.ArrayBuffer.empty[UcxShuffleBockId]

        // 1. Deserialize blockIds from header
        while (buffer.remaining() > 0) {
          blockIds += UcxShuffleBockId.deserialize(buffer)
        }

        amData.close()

        val blocks = blockIds.map{ bid => {
          new Block {
            private[this] val blockBuffer = blockManager.getBlockData(
              clientWorker.appId.toString, clientWorker.exeId.toString,
              bid.shuffleId, bid.mapId, bid.reduceId)
            private[this] val blockChannel = Channels.newChannel(
              blockBuffer.createInputStream)
            override def getBlock(byteBuffer: ByteBuffer): Unit = {
              blockChannel.read(byteBuffer)
            }

            override def getSize: Long = blockBuffer.size()
          }
        }}
        server.workerWrapper.handleFetchBlockRequest(clientWorker, replyTag, blocks)
      }
    })
  }

  @inline
  def selectServerThread(): ExternalUcxWorkerThread = allocatedServerThreads(
    (serverThreadId.incrementAndGet() % allocatedServerThreads.length).abs)
}
