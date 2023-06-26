/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.rpc

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.shuffle.ucx.UcxShuffleTransport
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages.{ExecutorAdded, IntroduceAllExecutors}
import org.apache.spark.shuffle.ucx.utils.SerializableDirectBuffer

import java.util.concurrent.ExecutorService

class UcxExecutorRpcEndpoint(override val rpcEnv: RpcEnv, transport: UcxShuffleTransport,
                             executorService: ExecutorService)
  extends RpcEndpoint  with Logging {

  override def receive: PartialFunction[Any, Unit] = {
    case ExecutorAdded(executorId: Long, _: RpcEndpointRef,
    ucxWorkerAddress: SerializableDirectBuffer) =>
      logDebug(s"Received ExecutorAdded($executorId)")
      transport.addExecutor(executorId, ucxWorkerAddress.value) // <zzh> may need to use ucxWorkerAddress instead of its value
    case IntroduceAllExecutors(executorIdToWorkerAdresses: Map[Long, SerializableDirectBuffer]) =>
      // logDebug(s"Received IntroduceAllExecutors(${executorIdToWorkerAdresses.keys.mkString(",")}")
      val startTime = System.currentTimeMillis()
      transport.addExecutors(executorIdToWorkerAdresses)
      logInfo(s"<zzh> IntroduceAllExecutors cost ${System.currentTimeMillis() - startTime}ms")
  }
}
