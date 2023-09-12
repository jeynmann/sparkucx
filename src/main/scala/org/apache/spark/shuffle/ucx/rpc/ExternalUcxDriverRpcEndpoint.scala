/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.rpc

import scala.collection.immutable.HashMap
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.rpc._
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages.{PushServiceAddress, PushAllServiceAddress}
import org.apache.spark.shuffle.ucx.utils.SerializableDirectBuffer

class UcxDriverRpcEndpoint(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint with Logging {

  private val endpoints = mutable.HashSet.empty[RpcEndpointRef]
  private var shuffleServerSet = mutable.HashSet.empty[Long, SerializableDirectBuffer]


  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case message@PushServiceAddress(shuffleServer: SerializableDirectBuffer, endpoint: RpcEndpointRef) => {
      // Driver receives a message from executor with it's workerAddress
      // 1. Introduce existing members of a cluster
      logDebug(s"Received $message")
      if (shuffleServerSet.nonEmpty) {
        val msg = PushAllServiceAddress(shuffleServerSet)
        logDebug(s"replying $msg to $endpoint")
        context.reply(msg)
      }
      // 2. For each existing member introduce newly joined executor.
      if (!shuffleServerSet.contains(shuffleServer)) {
        shuffleServerSet += shuffleServer
        endpoints.foreach(ep => {
          logDebug(s"Sending $message to $ep")
          ep.send(message)
        })
        logDebug(s"Connecting back to address: ${context.senderAddress}")
      }
      // 3. Add ep to registered eps.
      endpoints.add(endpoint)
    }
  }
}
