/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.rpc

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.shuffle.ucx.UcxShuffleTransportClient
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages.{PushServiceAddress, PushAllServiceAddress}
import org.apache.spark.shuffle.ucx.utils.SerializableDirectBuffer

import java.util.concurrent.ExecutorService

class ExternalUcxExecutorRpcEndpoint(override val rpcEnv: RpcEnv, transport: UcxShuffleTransportClient,
                             executorService: ExecutorService)
  extends RpcEndpoint  with Logging {

  override def receive: PartialFunction[Any, Unit] = {
    case PushServiceAddress(shuffleServer: SerializableDirectBuffer, endpoint: RpcEndpointRef) =>
      logDebug(s"Received PushServiceAddress($shuffleServer)")
      executorService.submit(new Runnable {
        override def run(): Unit = transport.connect(shuffleServer)
      })
    case PushAllServiceAddress(shuffleServerSet: Set[SerializableDirectBuffer]) =>
      logDebug(s"Received PushAllServiceAddress(${shuffleServerSet}")
      executorService.submit(new Runnable {
        override def run(): Unit = transport.connectAll(shuffleServerSet)
      })
  }
}
