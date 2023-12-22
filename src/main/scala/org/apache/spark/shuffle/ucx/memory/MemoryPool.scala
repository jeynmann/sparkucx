/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.memory

import java.io.Closeable
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedDeque}

import org.openucx.jucx.ucp.{UcpContext, UcpMemMapParams, UcpMemory}
import org.openucx.jucx.ucs.UcsConstants
import org.apache.spark.shuffle.utils.{UcxUtils, UcxLogging, UnsafeUtils}
import org.apache.spark.shuffle.ucx.MemoryBlock
import java.util.concurrent.Semaphore

class UcxBounceBufferMemoryBlock(private[ucx] val memory: UcpMemory, private[ucx] val refCount: AtomicInteger,
                                 private[ucx] val memPool: MemoryPool,
                                 override val address: Long, override val size: Long)
  extends MemoryBlock(address, size, memory.getMemType == UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST) {

  override def close(): Unit = {
    memPool.put(this)
  }
}


/**
 * Base class to implement memory pool
 */
case class MemoryPool(ucxContext: UcpContext, memoryType: Int)
  extends Closeable with UcxLogging {
  protected var minBufferSize: Long = 4096L
  protected var minRegistrationSize: Long = 1024L * 1024

  protected def roundUpToTheNextPowerOf2(size: Long): Long = {
    if (size < minBufferSize) {
      minBufferSize
    } else {
      // Round up length to the nearest power of two
      var length = size
      length -= 1
      length |= length >> 1
      length |= length >> 2
      length |= length >> 4
      length |= length >> 8
      length |= length >> 16
      length += 1
      length
    }
  }

  protected val allocatorMap = new ConcurrentHashMap[Long, AllocatorStack]()

  private val memPool = this

  protected case class AllocatorStack(length: Long, memType: Int) extends Closeable {
    logInfo(s"Allocator stack of memType: $memType and size $length")
    private val stack = new ConcurrentLinkedDeque[UcxBounceBufferMemoryBlock]
    private val numAllocs = new AtomicInteger(0)
    private val memMapParams = new UcpMemMapParams().allocate().setMemoryType(memType).setLength(length)

    private[memory] def get: UcxBounceBufferMemoryBlock = {
      var result = stack.pollFirst()
      if (result == null) {
        numAllocs.incrementAndGet()
        if (length < minRegistrationSize) {
          while (result == null) {
            preallocate((minRegistrationSize / length).toInt)
            result = stack.pollFirst()
          }
        } else {
          logTrace(s"Allocating buffer of size $length.")
          val memory = ucxContext.memoryMap(memMapParams)
          result = new UcxBounceBufferMemoryBlock(memory, new AtomicInteger(1), memPool,
            memory.getAddress, length)
        }
      }
      result
    }

    private[memory] def put(block: UcxBounceBufferMemoryBlock): Unit = {
      stack.add(block)
    }

    private[memory] def preallocate(numBuffers: Int): Unit = {
      logTrace(s"PreAllocating $numBuffers of size $length, " +
        s"totalSize: ${UcxUtils.bytesToString(length * numBuffers)}.")
      val memory = ucxContext.memoryMap(
        new UcpMemMapParams().allocate().setMemoryType(memType).setLength(length * numBuffers))
      val refCount = new AtomicInteger(numBuffers)
      var offset = 0L
      (0 until numBuffers).foreach(_ => {
        stack.add(new UcxBounceBufferMemoryBlock(memory, refCount, memPool, memory.getAddress + offset, length))
        offset += length
      })
    }

    override def close(): Unit = {
      var numBuffers = 0
      stack.forEach(block => {
        block.refCount.decrementAndGet()
        if (block.memory.getNativeId != null) {
          block.memory.deregister()
        }
        numBuffers += 1
      })
      logInfo(s"Closing $numBuffers buffers of size $length." +
        s"Number of allocations: ${numAllocs.get()}. Total size: ${UcxUtils.bytesToString(length * numBuffers)}")
      stack.clear()
    }
  }

  override def close(): Unit = {
    allocatorMap.values.forEach(allocator => allocator.close())
    allocatorMap.clear()
  }

  def get(size: Long): MemoryBlock = {
    val allocatorStack = allocatorMap.computeIfAbsent(roundUpToTheNextPowerOf2(size),
      s => AllocatorStack(s, memoryType))
    val result = allocatorStack.get
    new UcxBounceBufferMemoryBlock(result.memory, result.refCount, memPool, result.address, size)
  }

  def put(mem: MemoryBlock): Unit = {
    mem match {
      case m: UcxBounceBufferMemoryBlock =>
        val allocatorStack = allocatorMap.get(roundUpToTheNextPowerOf2(mem.size))
        allocatorStack.put(m)
      case _ => logWarning(s"Unknown memory block $mem")
    }
  }

  def preAllocate(size: Long, numBuffers: Int): Unit = {
    val roundedSize = roundUpToTheNextPowerOf2(size)
    val allocatorStack = allocatorMap.computeIfAbsent(roundedSize,
      s => AllocatorStack(s, memoryType))
    allocatorStack.preallocate(numBuffers)
  }
}

class UcxHostBounceBuffersPool(ucxContext: UcpContext)
  extends MemoryPool(ucxContext, UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST) {
  def init(minRegSize: Long, minBufSize: Long, preAllocMap: Map[Long, Int]): Unit = {
    minRegistrationSize = minRegSize
    minBufferSize = minBufSize
    preAllocMap.foreach{
      case (bufferSize, count) => preAllocate(bufferSize, count)
    }
  }
}

class UcxMemBlock(private[ucx] val memory: UcpMemory,
                  private[ucx] val allocator: UcxMemoryAllocator,
                  override val address: Long, override val size: Long)
  extends MemoryBlock(address, size, memory.getMemType == UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST) {

  lazy val byteBuffer = UnsafeUtils.getByteBufferView(address, size.min(Int.MaxValue).toInt)

  override def close(): Unit = {
    allocator.deallocate(this)
  }
}

class UcxLinkedMemBlock(private[memory] val superMem: UcxLinkedMemBlock,
                        private[memory] var broMem: UcxLinkedMemBlock,
                        override private[ucx] val memory: UcpMemory,
                        override private[ucx] val allocator: UcxMemoryAllocator,
                        override val address: Long, override val size: Long)
  extends UcxMemBlock(memory, allocator, address, size) {
}

trait UcxMemoryAllocator extends Closeable {
  def allocate(): UcxMemBlock
  def deallocate(mem: UcxMemBlock): Unit
  def preallocate(numBuffers: Int): Unit = {
    (0 until numBuffers).map(x => allocate()).foreach(_.close())
  }
}

class UcxBaseMemAllocator extends UcxMemoryAllocator with UcxLogging {
  private[memory] val stack = new ConcurrentLinkedDeque[UcxMemBlock]
  private[memory] val numAllocs = new AtomicInteger(0)
  private[memory] val memMapParams = new UcpMemMapParams().allocate()
    .setMemoryType(UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)

  override def allocate(): UcxMemBlock = ???
  override def deallocate(mem: UcxMemBlock): Unit = ???
  override def close(): Unit = {
    var numBuffers = 0
    var length = 0L
    stack.forEach(block => {
      if (block.memory.getNativeId != null) {
        length = block.size
        block.memory.deregister()
      }
      numBuffers += 1
    })
    if (numBuffers != 0) {
      logInfo(s"Closing $numBuffers buffers size $length allocations " +
        s"${numAllocs.get()}. Total ${UcxUtils.bytesToString(length * numBuffers)}")
      stack.clear()
    }
  }
}

case class UcxLinkedMemAllocator(length: Long, minRegistrationSize: Long,
                                 next: UcxLinkedMemAllocator,
                                 ucxContext: UcpContext)
  extends UcxBaseMemAllocator() with Closeable {
  private[this] lazy val registrationSize = length.max(minRegistrationSize)
  private[this] var limit: Semaphore = _
  logInfo(s"Allocator stack size $length")
  if (next == null) {
    memMapParams.setLength(registrationSize)
  }

  override def allocate(): UcxMemBlock = {
    acquireLimit()
    var result = stack.pollFirst()
    if (result != null) {
      result
    } else if (next == null) {
      logTrace(s"Allocating buffer of size $length.")
      while (result == null) {
        numAllocs.incrementAndGet()
        val memory = ucxContext.memoryMap(memMapParams)
        val numBuffers = memory.getLength / length
        var address = memory.getAddress
        for (i <- 0L until numBuffers) {
          stack.add(new UcxLinkedMemBlock(null, null, memory, this, address, length))
          address += length
        }
        result = stack.pollFirst()
      }
      result
    } else {
      val superMem = next.allocate().asInstanceOf[UcxLinkedMemBlock]
      val address1 = superMem.address
      val address2 = address1 + length
      val block1 = new UcxLinkedMemBlock(superMem, null, superMem.memory, this,
                                         address1, length)
      val block2 = new UcxLinkedMemBlock(superMem, null, superMem.memory, this,
                                         address2, length)
      block1.broMem = block2
      block2.broMem = block1
      stack.add(block2)
      block1
    }
  }

  override def deallocate(memBlock: UcxMemBlock): Unit = {
    val block = memBlock.asInstanceOf[UcxLinkedMemBlock]
    if ((block.superMem == null) || (!stack.remove(block.broMem))) {
      stack.add(block)
    } else {
      next.deallocate(block.superMem)
    }
    releaseLimit()
  }

  def setLimit(num: Int): Unit = {
    limit = new Semaphore(num)
  }

  def acquireLimit() = if (limit != null) {
    limit.acquire(1)
  }

  def releaseLimit() = if (limit != null) {
    limit.release(1)
  }
}

case class UcxMemPool(ucxContext: UcpContext)
  extends Closeable with UcxLogging {
  private[memory] val memRange = (1 until 29).map(1 << _).reverse
  private[memory] val memGroupSize = 3
  private[memory] var minBufferSize: Long = 4096L
  private[memory] var maxBufferSize: Long = 2L * 1024 * 1024 * 1024
  private[memory] var minRegistrationSize: Long = 1024L * 1024
  private[memory] var maxRegistrationSize: Long = 16L * 1024 * 1024 * 1024

  def init(minBufSize: Long, maxBufSize: Long, minRegSize: Long, maxRegSize: Long, preAllocMap: Map[Long, Int]):
    Unit = {
    minBufferSize = roundUpToTheNextPowerOf2(minBufSize)
    maxBufferSize = roundUpToTheNextPowerOf2(maxBufSize)
    minRegistrationSize = roundUpToTheNextPowerOf2(minRegSize)
    maxRegistrationSize = roundUpToTheNextPowerOf2(maxRegSize / 2)
    val minLimit = (maxRegistrationSize / maxBufferSize).max(1L)
                                                        .min(Int.MaxValue)
                                                        .toInt
    logInfo(s"UcxMemPool limit $minLimit buf ($minBufferSize, $maxBufferSize)" +
            s"reg ($minRegistrationSize, $maxRegistrationSize)")
    for (i <- 0 until memRange.length by memGroupSize) {
      var superAllocator: UcxLinkedMemAllocator = null
      for (j <- 0 until memGroupSize.min(memRange.length - i)) {
        val memSize = memRange(i + j)
        if ((memSize >= minBufSize) && (memSize <= maxBufferSize)) {
          val current = new UcxLinkedMemAllocator(memSize, minRegistrationSize,
                                                  superAllocator, ucxContext)
          superAllocator = current
          allocatorMap.put(memSize, current)
          // set limit to top allocator
          if (superAllocator == null) {
            current.setLimit(minLimit << i)
          }
        }
      }
    }
    preAllocMap.foreach{
      case (size, count) => {
        allocatorMap.get(roundUpToTheNextPowerOf2(size)).preallocate(count)
      }
    }
  }

  protected def roundUpToTheNextPowerOf2(size: Long): Long = {
    if (size < minBufferSize) {
      minBufferSize
    } else {
      // Round up length to the nearest power of two
      var length = size
      length -= 1
      length |= length >> 1
      length |= length >> 2
      length |= length >> 4
      length |= length >> 8
      length |= length >> 16
      length += 1
      length
    }
  }

  protected val allocatorMap = new ConcurrentHashMap[Long, UcxMemoryAllocator]()

  override def close(): Unit = {
    allocatorMap.values.forEach(allocator => allocator.close())
    allocatorMap.clear()
  }

  def get(size: Long): MemoryBlock = {
    allocatorMap.get(roundUpToTheNextPowerOf2(size)).allocate()
  }
}
