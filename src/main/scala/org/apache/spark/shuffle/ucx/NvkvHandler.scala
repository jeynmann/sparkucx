package org.apache.spark.shuffle.ucx

import java.util.zip.CRC32
import scala.collection.mutable.ListBuffer
import org.openucx.jnvkv._
import org.openucx.jucx.ucp._
import org.openucx.jucx.UcxException
import org.openucx.jucx.ucs.UcsConstants
import java.nio.ByteBuffer
import java.nio.ByteOrder
import org.apache.spark.SparkEnv
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.internal.Logging
import java.nio.BufferOverflowException
import org.openucx.jnvkv.NvkvException

object NvkvHandler {
  private var worker: NvkvHandler = null

  def getHandler(ucxContext: UcpContext, numOfPartitions: Int): NvkvHandler = {
    if (worker == null) worker = new NvkvHandler(ucxContext, numOfPartitions)
    worker
  }
}

class NvkvHandler private(ucxContext: UcpContext, private var numOfPartitions: Long) extends Logging {
  final private val nvkvWriteBufferSize  = 1024*1024*40L
  final private val nvkvReadBufferSize  = 1024*1024*40L
  final private val nvkvRemoteReadBufferSize = 1024*1024*43L
  final private val nvkvNumOfReadBuffers = 16
  final private val alignment = 512
  private var nvkvWriteBuffer: ByteBuffer = null
  private var nvkvReadBuffer: ByteBuffer = null
  private var nvkvRemoteReadBuffer: ByteBuffer = null
  private var nvkv: Nvkv.Context = null
  private var ds_idx = 0
  private var nvkvStorageSize = 0L
  private var partitionSize = 0L
  private var packData: ByteBuffer = null
  val numOfMappers  = SparkEnv.get.conf.getInt("spark.groupByTest.numMappers", 1)
  val numOfReducers = SparkEnv.get.conf.getInt("spark.groupByTest.numReducers", 1)
  private var reducePartitions: Array[Array[ReducePartition]] = Array.ofDim[ReducePartition](numOfMappers, numOfReducers)
  val nvkvLogEnabled = SparkEnv.get.conf.getInt("spark.nvkvLogs.enabled", 0)
  val blockManager = SparkEnv.get.blockManager.blockManagerId
  val executerId = blockManager.executorId.toInt

  private def nvkvLogDebug(msg: => String) {
    if (nvkvLogEnabled == 1) {
      logDebug(msg)
    }
  }

  nvkvLogDebug(s"LEO NvkvHandler constructor")
  try {
    Nvkv.init("mlx5_0", nvkvLogEnabled, (1 << executerId).toHexString) 
  } catch {
    case e: NvkvException => logDebug(s"LEO NvkvHandler: Failed to init nvkv")
  }

  val ds: Array[Nvkv.DataSet] = Nvkv.query()
  if (ds.length == 0) throw new NvkvException("Failed to detect nvkv device!")

  try {
    nvkv = Nvkv.open(ds, Nvkv.LOCAL|Nvkv.REMOTE)
  } catch {
    case e: NvkvException => logDebug(s"LEO NvkvHandler: Failed to open nvkv")
  }

  try {
    nvkvWriteBuffer = nvkv.alloc(nvkvWriteBufferSize)
  } catch {
    case e: NvkvException => logDebug(s"LEO NvkvHandler: Failed to allocate nvkv write buffer")
  }
  
  // nvkvReadBuffer = nvkv.alloc(nvkvReadBufferSize)
  try {
    nvkvRemoteReadBuffer = nvkv.alloc(nvkvNumOfReadBuffers*nvkvRemoteReadBufferSize)
  } catch {
    case e: NvkvException => logDebug(s"LEO NvkvHandler: Failed to allocate nvkv read buffer")
  }

  nvkvStorageSize = ds(0).size
  partitionSize = nvkvStorageSize / numOfPartitions

  var nvkvCtx: Array[Byte] = ByteBuffer.wrap(nvkv.export()).order(ByteOrder.nativeOrder()).array()
  var nvkvCtxSize: Int = nvkvCtx.length

  nvkvLogDebug(s"LEO Register bb")
  val mem: UcpMemory = ucxContext.registerMemory(nvkvRemoteReadBuffer)
  var mkeyBuffer: ByteBuffer = null
  mkeyBuffer = mem.getExportedMkeyBuffer()
  
  nvkvLogDebug(s"LEO Try to pack nvkv")
  packData = ByteBuffer.allocateDirect(UnsafeUtils.INT_SIZE  + // nvkvCtx size
                                       nvkvCtxSize           + // nvkvCtx
                                       UnsafeUtils.LONG_SIZE + // readBuf Address
                                       UnsafeUtils.LONG_SIZE + // readBuf length
                                       UnsafeUtils.INT_SIZE  + // max block size
                                       UnsafeUtils.INT_SIZE  + // readBuffer mkey size
                                       mkeyBuffer.capacity()).order(ByteOrder.nativeOrder())
  packData.putInt(nvkvCtxSize)
  packData.put(nvkvCtx)
  packData.putLong(UnsafeUtils.getAdress(nvkvRemoteReadBuffer))
  packData.putLong(nvkvNumOfReadBuffers*nvkvRemoteReadBufferSize)
  packData.putInt(nvkvRemoteReadBufferSize.toInt)
  packData.putInt(mkeyBuffer.capacity())
  packData.put(mkeyBuffer)
  packData.rewind()

  nvkvLogDebug(s"LEO packedNvkv nvkvCtx ${nvkvCtx} nvkvCtxSize ${nvkvCtxSize} bb ${UnsafeUtils.getAdress(nvkvRemoteReadBuffer)}")
  nvkvLogDebug(s"LEO packedNvkv packData capacity ${mkeyBuffer.capacity()} packData limit ${mkeyBuffer.limit()}")

  // TODO - store bb in array not ListBuffer
  // To be used when fetching local blocks using DPU - no need to transfer data just bb idx
  def createMPFromBuffer(buffer: ByteBuffer, bbSize: Int): ListBuffer[ByteBuffer] = {
    var mpool: ListBuffer[ByteBuffer] = ListBuffer()

    var pos = 0
    while (pos < buffer.capacity()) {
      var bb: ByteBuffer = buffer.slice()
      bb.limit(bbSize)
      mpool.append(bb)
      pos += bbSize
      buffer.position(pos)
    }

    buffer.position(0)
    mpool
  }

  def pack: ByteBuffer = packData

  def connectToRemote(add: Array[Byte]): Unit = {
    try {
      nvkv.connect(add)
    } catch {
      case e: NvkvException => logDebug("LEO NvkvHandler: Failed to connect to remote nvkv received from DPU")
    }
  }
  
  protected class Request(private var buffer: ByteBuffer, private var length: Long, private var offset: Long) {
    private var complete = false
    def setComplete(): Unit = {
      complete = true
    }

    def getComplete(): Boolean = {
      complete
    }

    def getLength: Long = length
    def getOffset: Long = offset
    def getBuffer: ByteBuffer = buffer
  }

  class WriteRequest(private var dsIdx: Int, private var source: ByteBuffer, length: Long, offset: Long) extends Request(source, length, offset) {
    def getDsIdx: Int = dsIdx
  }

  class ReadRequest(private var dest: ByteBuffer, length: Long, offset: Long) extends Request(dest, length, offset) {
  }

  private class ReducePartition(private var offset: Long, private var length: Long) {
    def getOffset: Long = offset
    def getLength: Long = length
  }

  private def post(request: WriteRequest) = {
    nvkvLogDebug(s"LEO NvkvHandler post write")
    if (request.getOffset % 512 != 0) {
      throw new IllegalArgumentException(s"write Illegal offset ${request.getOffset}")
    }
    
    if (request.getLength % 512 != 0) {
      throw new IllegalArgumentException(s"write Illegal length ${request.getLength}")
    }

    try nvkv.postWrite(request.getDsIdx, nvkvWriteBuffer, 0, request.getLength, request.getOffset, new Nvkv.Context.Callback() {
      def done(): Unit = {
        request.setComplete()
        nvkvLogDebug(s"LEO NvkvHandler post completed!")
      }
    })
    catch {
      case e: Exception =>
        throw new RuntimeException(e)
    }
  }

  private def post(request: ReadRequest) = {
    nvkvLogDebug(s"LEO NvkvHandler post read")
    try nvkv.postRead(ds_idx, request.getBuffer, 0, request.getLength, request.getOffset, new Nvkv.Context.Callback() {
      def done(): Unit = {
        request.setComplete()
        nvkvLogDebug(s"LEO NvkvHandler post completed!")
      }
    })
    catch {
      case e: Exception =>
        throw new RuntimeException(e)
    }
  }

  def pollCompletion(request: Request): Unit = {
    while (!request.getComplete()) nvkv.progress
  }

  def getPartitionSize: Long = partitionSize
  def getNumOfDevices: Int = ds.length

  private def test(length: Int): Unit = {
    val nvkvWriteBufferTmp = nvkvWriteBuffer.duplicate
    nvkvWriteBufferTmp.limit(length)
    val nvkvReadBufferTmp = nvkvReadBuffer.duplicate
    nvkvReadBufferTmp.limit(length)
    if (!(nvkvWriteBufferTmp == nvkvReadBufferTmp)) throw new RuntimeException("Data is corrupted")
  }

  private def getAlignedLength(length: Int) = if (length % 512 == 0) {length} else {length + (alignment - (length % alignment))}

  def read(length: Int, offset: Long): ByteBuffer = {
    val alignedLength = getAlignedLength(length)
    nvkvLogDebug(s"LEO NvkvHandler read aligned size " + alignedLength)
    val readRequest = new ReadRequest(nvkvReadBuffer, alignedLength, offset)
    post(readRequest)
    pollCompletion(readRequest)
    nvkvLogDebug(s"LEO NvkvHandler read complete")

    var clone: ByteBuffer = ByteBuffer.allocateDirect(length)
    nvkvReadBuffer.limit(length)
    clone.put(nvkvReadBuffer)
    clone.rewind()
    clone.limit(length)
    nvkvReadBuffer.limit(nvkvReadBuffer.capacity())
    nvkvReadBuffer.rewind()
    clone
  }

  def write(dsIdx: Int, shuffleId: Int, mapId: Long, 
            reducePartitionId: Int, bytes: Array[Byte], length: Int, offset: Long): Unit = {
    val source: ByteBuffer = ByteBuffer.wrap(bytes)
    var relativeOffset: Long = offset - nvkvWriteBuffer.position()
    var sourceLimit: Int = 0
    var remain: Int = length

    while (remain > 0) {
        if (!nvkvWriteBuffer.hasRemaining()) {
            nvkvWriteBuffer.rewind()
            nvkvLogDebug(s"LEO NvkvHandler spill buffer relativeOffset $relativeOffset")
            val writeRequest = new WriteRequest(dsIdx, nvkvWriteBuffer, nvkvWriteBufferSize, relativeOffset)
            post(writeRequest)
            pollCompletion(writeRequest)
            nvkvLogDebug(s"LEO NvkvHandler write complete")

            // read(nvkvWriteBufferSize, relativeOffset)
            // test(nvkvWriteBufferSize)

            relativeOffset += nvkvWriteBufferSize
        } else {
            sourceLimit = (source.position()+nvkvWriteBuffer.remaining()).min(length)
            nvkvLogDebug(s"LEO source_position ${source.position()} buffer_remaining ${nvkvWriteBuffer.remaining()} source_capacity ${length}")
            nvkvLogDebug(s"LEO NvkvHandler write to buffer from offset ${source.position()} length ${sourceLimit - source.position()}")
            remain -= (sourceLimit - source.position());
            source.limit(sourceLimit)
            nvkvWriteBuffer.put(source)
        }
    }
  }

  def writeRemaining(dsIdx: Int, offset: Long): Int = {
    val bufferPosition = nvkvWriteBuffer.position()
    var relativeOffset: Long = offset - bufferPosition
    nvkvWriteBuffer.rewind()
    nvkvLogDebug(s"LEO NvkvHandler write remaining size ${bufferPosition} relativeOffset ${relativeOffset}")
    val writeRequest = new WriteRequest(dsIdx, nvkvWriteBuffer, getAlignedLength(bufferPosition), relativeOffset)
    post(writeRequest)
    pollCompletion(writeRequest)
    nvkvLogDebug(s"LEO NvkvHandler write complete")
    // read(bufferPosition, relativeOffset)
    // test(bufferPosition)
    (getAlignedLength(bufferPosition) - bufferPosition)
  }

  def commitPartition(dsIdx: Int, start: Long, length: Long, shuffleId: Int, 
                      mapId: Long, reducePartitionId: Int): Unit = {
    nvkvLogDebug(s"LEO NvkvHandler commitPartition $shuffleId,$mapId,$reducePartitionId offset $start length $length dsIdx $dsIdx")
    reducePartitions(mapId.toInt)(reducePartitionId) = new ReducePartition(start+dsIdx*nvkvStorageSize, length)
  }

  def getPartitonOffset(shuffleId: Int, mapId: Long, reducePartitionId: Int): Long = {
    nvkvLogDebug(s"LEO NvkvHandler getPartitionOffset $shuffleId,$mapId,$reducePartitionId")
    if (reducePartitions(mapId.toInt)(reducePartitionId) == null) {
      0
    } else {
      reducePartitions(mapId.toInt)(reducePartitionId).getOffset
    }
  }

  def getPartitonLength(shuffleId: Int, mapId: Long, reducePartitionId: Int): Long = {
    nvkvLogDebug(s"LEO NvkvHandler getPartitionOffset $shuffleId,$mapId,$reducePartitionId")
    if (reducePartitions(mapId.toInt)(reducePartitionId) == null) {
      0
    } else {
      reducePartitions(mapId.toInt)(reducePartitionId).getLength
    }
  }
}
