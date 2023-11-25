/*
* Copyright (C) Mellanox Technologies Ltd. 2020. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Success

import org.apache.spark.rpc.RpcEnv
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.shuffle.ucx.rpc.{UcxDriverRpcEndpoint, UcxExecutorRpcEndpoint}
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages.{ExecutorAdded, IntroduceAllExecutors, NvkvRequestLock}
import org.apache.spark.shuffle.ucx.utils.{SerializableDirectBuffer, SerializationUtils, DpuUtils}
import org.apache.spark.shuffle.utils.{UnsafeUtils, CommonUtils}
import org.apache.spark.util.{RpcUtils, ThreadUtils}
import org.apache.spark.{SecurityManager, SparkConf, SparkEnv}
import scala.concurrent.duration._

/**
 * Common part for all spark versions for UcxShuffleManager logic
 */
abstract class CommonUcxShuffleManager(val conf: SparkConf, isDriver: Boolean) extends SortShuffleManager(conf) {
  type ShuffleId = Int
  type MapId = Int
  type ReduceId = Long

  val ucxShuffleConf = new UcxShuffleConf(conf)

  @volatile var ucxTransport: UcxShuffleTransport = _

  private var executorEndpoint: UcxExecutorRpcEndpoint = _
  private var driverEndpoint: UcxDriverRpcEndpoint = _

  protected val driverRpcName = "SparkUCX_driver"

  private val setupThread = ThreadUtils.newDaemonSingleThreadExecutor("UcxTransportSetupThread")

  logInfo("CommonUcxShuffleManager")

  def getTransport(): UcxShuffleTransport = { ucxTransport }
  def setTransport(transport: UcxShuffleTransport): Unit = { ucxTransport = transport }

  setupThread.execute(new Runnable {
    override def run(): Unit = {
      CommonUtils.safePolling(() => {},
      () => {SparkEnv.get == null}, 10.seconds.fromNow,
      "Got timeout when polling", Duration(10, "millis"))

      if (isDriver) {
        val rpcEnv = SparkEnv.get.rpcEnv
        logInfo(s"Setting up driver RPC")
        driverEndpoint = new UcxDriverRpcEndpoint(rpcEnv)
        rpcEnv.setupEndpoint(driverRpcName, driverEndpoint)
      } else {
        CommonUtils.safePolling(() => {},
          () => {SparkEnv.get.blockManager.blockManagerId == null},
          10.seconds.fromNow,
          "Got timeout when polling", Duration(5, "millis"))
        startUcxTransport()
      }
    }
  })

  /**
   * Atomically starts UcxNode singleton - one for all shuffle threads.
   */
  def startUcxTransport(): Unit = if (ucxTransport == null) {
    logInfo("startUcxTransport")
    val blockManager = SparkEnv.get.blockManager.blockManagerId
    val transport = new UcxShuffleTransport(ucxShuffleConf, blockManager.executorId.toLong)
    transport.init()

    val rpcEnv = RpcEnv.create("ucx-rpc-env", blockManager.host, blockManager.port,
      conf, new SecurityManager(conf), clientMode = false)
    val driverEndpoint = RpcUtils.makeDriverRef(driverRpcName, conf, rpcEnv)

    executorEndpoint = new UcxExecutorRpcEndpoint(rpcEnv, transport, setupThread,
        ucxTransport, driverEndpoint, blockManager.executorId.toLong, setTransport)
    val endpoint = rpcEnv.setupEndpoint(
      s"ucx-shuffle-executor-${blockManager.executorId}",
      executorEndpoint)
    logInfo(s"startUcxTransport sending RPC IntroduceAllExecutors")

    var sockAddress = DpuUtils.getLocalDpuSocketAddress()
    logInfo(s"Local DPU Socket Address ${sockAddress}")
    driverEndpoint.ask[IntroduceAllExecutors](ExecutorAdded(blockManager.executorId.toLong, endpoint,
      new SerializableDirectBuffer(SerializationUtils.serializeInetAddress(sockAddress))))
      .andThen {
        case Success(msg) =>
          logInfo(s"Receive reply $msg")
          executorEndpoint.receive(msg)
      }

    driverEndpoint.send(NvkvRequestLock(blockManager.executorId.toLong, endpoint))
  }


  override def unregisterShuffle(shuffleId: Int): Boolean = {
    shuffleBlockResolver.asInstanceOf[CommonUcxShuffleBlockResolver].removeShuffle(shuffleId)
    super.unregisterShuffle(shuffleId)
  }

  /**
   * Called on both driver and executors to finally cleanup resources.
   */
  override def stop(): Unit = synchronized {
    super.stop()
    if (ucxTransport != null) {
      ucxTransport.close()
      ucxTransport = null
    }
    if (executorEndpoint != null) {
      executorEndpoint.stop()
    }
    if (driverEndpoint != null) {
      driverEndpoint.stop()
    }
    setupThread.shutdown()
  }

}
