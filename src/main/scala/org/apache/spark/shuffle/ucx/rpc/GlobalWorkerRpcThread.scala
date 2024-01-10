/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.rpc

import java.nio.ByteBuffer
import org.openucx.jucx.ucp.{UcpAmData, UcpConstants, UcpEndpoint, UcpWorker}
import org.openucx.jucx.ucs.UcsConstants
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.{UcxShuffleTransport, UcxShuffleBockId}
import org.apache.spark.shuffle.utils.UnsafeUtils

class GlobalWorkerRpcThread(globalWorker: UcpWorker, transport: UcxShuffleTransport)
  extends Thread with Logging {
  setDaemon(true)
  setName("Global worker progress thread")

  override def run(): Unit = {
    if (transport.ucxShuffleConf.useWakeup) {
      while (!isInterrupted) {
        while (globalWorker.progress != 0) {}
        globalWorker.waitForEvents()
      }
    } else {
      while (!isInterrupted) {
        while (globalWorker.progress != 0) {}
      }
    }
  }
}
