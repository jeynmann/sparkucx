package org.apache.spark.network.shuffle

import java.io.File
import java.nio.ByteBuffer

import org.apache.spark.network.server.OneForOneStreamManager
import org.apache.spark.network.util.TransportConf
import org.apache.spark.network.client.{TransportClient, RpcResponseCallback}
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage

import org.apache.spark.shuffle.utils.UcxLogging
import org.apache.spark.shuffle.ucx.ExternalUcxServerTransport
import org.apache.spark.shuffle.ucx.{UcxProtocol, UcxQueryService, UcxServiceInfo}

class ExternalUcxShuffleBlockHandler(conf: TransportConf, registeredExecutorFile: File)
  extends ExternalShuffleBlockHandler(new OneForOneStreamManager(),
  new ExternalUcxShuffleBlockResolver(conf, registeredExecutorFile)) with UcxLogging {

  val ucxBlockManager = blockManager.asInstanceOf[ExternalUcxShuffleBlockResolver]

  def setTransport(transport: ExternalUcxServerTransport): Unit = {
    ucxBlockManager.setTransport(transport)
  }

  override def receive(client: TransportClient, message: ByteBuffer,
                       callback: RpcResponseCallback): Unit = {
    val ucxMsg = UcxProtocol.fromByteBuffer(message)
    if (ucxMsg.isInstanceOf[UcxQueryService]) {
      val reply = new UcxServiceInfo(ucxBlockManager.ucxTransport.workerAddress)
      callback.onSuccess(reply.toByteBuffer());
      return
    }

    val sparkMsg = BlockTransferMessage.Decoder.fromByteBuffer(message)
    handleMessage(sparkMsg, client, callback)
  }
}