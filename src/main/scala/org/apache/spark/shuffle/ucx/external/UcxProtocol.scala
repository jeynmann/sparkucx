package org.apache.spark.shuffle.ucx

import java.nio.ByteBuffer

trait UcxProtocol {
  def msgType(): Byte
  def encodedLength(): Int
  def encode(buf: ByteBuffer): Unit

  def toByteBuffer(): ByteBuffer = {
    val buf = ByteBuffer.allocate(encodedLength() + 1)
    buf.put(msgType())
    encode(buf)
    buf.rewind()
    buf
  }
}

object UcxProtocol {
  final val UCX_QUERY_SERVICE = 64
  final val UCX_SERVICE_INFO = 65
  def fromByteBuffer(msg: ByteBuffer): UcxProtocol = {
    val msgtype = msg.get()
    msgtype match {
      case UCX_QUERY_SERVICE => UcxQueryService.decode(msg)
      case UCX_SERVICE_INFO => UcxServiceInfo.decode(msg)
      case _ => { msg.rewind(); null }
    }
  }
}

class UcxQueryService extends UcxProtocol {
  override def msgType(): Byte = UcxProtocol.UCX_QUERY_SERVICE.toByte

  override def encodedLength(): Int = 0

  override def encode(buf: ByteBuffer): Unit = {}
}

object UcxQueryService {
  def decode(buf: ByteBuffer): UcxQueryService = {
    new UcxQueryService()
  }
}

class UcxServiceInfo(val ucpAddress: ByteBuffer) extends UcxProtocol {
  override def msgType(): Byte = UcxProtocol.UCX_SERVICE_INFO.toByte

  override def encodedLength(): Int = ucpAddress.remaining()

  override def encode(buf: ByteBuffer): Unit = {
    buf.put(ucpAddress)
  }
}

object UcxServiceInfo {
  def decode(buf: ByteBuffer): UcxServiceInfo = {
    val ucpAddress = ByteBuffer.allocateDirect(buf.remaining)
    ucpAddress.put(buf)
    ucpAddress.rewind()
    new UcxServiceInfo(ucpAddress)
  }
}