/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.rpc

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.shuffle.ucx.BlockId
import org.apache.spark.shuffle.ucx.utils.SerializableDirectBuffer

object UcxRpcMessages {
  /**
   * Called from executor to driver, to introduce ucx worker address.
   */
  case class ExecutorAdded(executorId: Long, endpoint: RpcEndpointRef,
                           ucxWorkerAddress: SerializableDirectBuffer)

  /**
   * Reply from driver with all executors in the cluster with their worker addresses.
   */
  case class IntroduceAllExecutors(executorIdToAddress: Map[Long, SerializableDirectBuffer])

  case class PushServiceAddress(host: String, ports: Seq[Int], endpoint: RpcEndpointRef)

  case class PushAllServiceAddress(shuffleServerMap: Map[String, Seq[Int]])
}
