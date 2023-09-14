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
  private var shuffleServerMap = mutable.HashMap.empty[InetSocketAddress, SerializableDirectBuffer]


  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case message@PushServiceAddress(addressBuffer: SerializableDirectBuffer, endpoint: RpcEndpointRef) => {
      // Driver receives a message from executor with it's workerAddress
      val shuffleServer = SerializationUtils.deserializeInetAddress(addressBuffer.value)
      logInfo(s"Received message $shuffleServer from ${context.senderAddress}")
      // 1. For each existing member introduce newly joined executor.
      if (!shuffleServerMap.contains(shuffleServer)) {
        shuffleServerMap += shuffleServer -> addressBuffer
        endpoints.foreach(ep => {
          logInfo(s"Sending message $shuffleServer to $ep")
          ep.send(message)
        })
      }
      // 2. Introduce existing members of a cluster
      if (shuffleServerMap.nonEmpty) {
        val msg = PushAllServiceAddress(shuffleServerMap.values.toSet)
        logInfo(s"Replying $msg to $endpoint")
        context.reply(msg)
      }
      // 3. Add ep to registered eps.
      endpoints.add(endpoint)
    }
  }
}
