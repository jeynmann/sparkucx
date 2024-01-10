/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.rpc

import java.net.InetSocketAddress

import scala.collection.immutable.HashMap
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.rpc._
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages.{PushServiceAddress, PushAllServiceAddress}
import org.apache.spark.shuffle.ucx.utils.{SerializableDirectBuffer, SerializationUtils}

class ExternalUcxDriverRpcEndpoint(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint with Logging {

  private val endpoints = mutable.HashSet.empty[RpcEndpointRef]
  private var serverAddressMap = HashMap.empty[SerializableDirectBuffer,
                                               SerializableDirectBuffer]

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case message@PushServiceAddress(serverBuffer: SerializableDirectBuffer,
                                    addressBuffer: SerializableDirectBuffer,
                                    endpoint: RpcEndpointRef) => {
      // Driver receives a message from executor with it's workerAddress
      val server = SerializationUtils.deserializeInetAddress(serverBuffer.value)
      logInfo(s"Received message $server from ${context.senderAddress}")
      // 1. For each existing member introduce newly joined executor.
      if (!serverAddressMap.contains(serverBuffer)) {
        serverAddressMap += serverBuffer -> addressBuffer
        endpoints.foreach(ep => {
          logDebug(s"Sending message $server to $ep")
          ep.send(message)
        })
      }
      // 2. Introduce existing members of a cluster
      if (serverAddressMap.nonEmpty) {
        val msg = PushAllServiceAddress(serverAddressMap)
        logDebug(s"Replying $msg to $endpoint")
        context.reply(msg)
      }
      // 3. Add ep to registered eps.
      endpoints.add(endpoint)
    }
  }
}
