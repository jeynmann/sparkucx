
package org.apache.spark.shuffle.ucx

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.HashMap

class Record[T](var value: T) {
}

class IntRecord extends Record[Int](0) {
}

object IntRecord {
    def add(r: Record[Int], x: Int): Unit = r.value += x
    def mul(r: Record[Int], x: Int): Unit = r.value *= x
}

class IntArrayRecord(size: Int) extends Record[Array[Int]](new Array[Int](size)) {
}

object IntArrayRecord {
    def add(r: Record[Array[Int]], x: (Int, Int)): Unit = r.value(x._1) += x._2
    def mul(r: Record[Array[Int]], x: (Int, Int)): Unit = r.value(x._1) *= x._2
}

class IntMapRecord extends Record[HashMap[Int,Int]](HashMap.empty[Int,Int]) {
}

object IntMapRecord {
    def add(r: Record[HashMap[Int,Int]], x: (Int, Int)): Unit = r.value.put(x._1, r.value.getOrElse(x._1, 0) + x._2)
    def mul(r: Record[HashMap[Int,Int]], x: (Int, Int)): Unit = r.value.put(x._1, r.value.getOrElse(x._1, 0) + x._2)
}

class LongRecord extends Record[Long](0) {
}

object LongRecord {
    def add(r: Record[Long], x: Long): Unit = r.value += x
    def mul(r: Record[Long], x: Long): Unit = r.value *= x
}

class Monitor[T] {
    val recordMap = new TrieMap[Record[T], Int]
    private val recordTls = new ThreadLocal[Record[T]] {
        override def initialValue = initialLocal
    }

    def initialLocal(): Record[T] = ???
    def update(op: (Record[T]) => Unit): Unit = {
        op(recordTls.get);
    }

    def aggregate(): T = ???
}

class IntMonitor extends Monitor[Int] {
    override def initialLocal(): Record[Int] = {
        val x = new IntRecord
        recordMap.getOrElseUpdate(x, 0)
        x
    }

    override def aggregate(): Int = {
        recordMap.map{ case (k,v) => k.value }.sum
    }

    override def toString(): String = s"${aggregate()}"
}

class IntArrayMonitor(size: Int) extends Monitor[Array[Int]] {
    override def initialLocal = {
        val x = new IntArrayRecord(size)
        recordMap.getOrElseUpdate(x, 0)
        x
    }

    override def aggregate(): Array[Int] = {
        val r = new Array[Int](size)
        recordMap.foreach {
            case (k,v) => {
                for (i <- 0 until size) {
                    r(i) += k.value(i)
                }
            }
        }
        r
    }

    override def toString(): String = s"${aggregate().toSeq}"
}

class IntMapMonitor(size: Int) extends Monitor[HashMap[Int,Int]] {
    override def initialLocal = {
        val x = new IntMapRecord
        recordMap.getOrElseUpdate(x, 0)
        x
    }

    override def aggregate(): HashMap[Int,Int] = {
        val r = HashMap.empty[Int,Int]
        recordMap.foreach {
            case (k,v) => {
                for ((kk, vv) <- k.value) {
                    r.put(kk, r.getOrElse(kk, 0) + vv)
                }
            }
        }
        r
    }

    override def toString(): String = s"${aggregate().toSeq}"
}

class LongMonitor extends Monitor[Long] {
    override def initialLocal(): Record[Long] = {
        val x = new LongRecord
        recordMap.getOrElseUpdate(x, 0)
        x
    }

    override def aggregate(): Long = {
        recordMap.map{ case (k,v) => k.value }.sum
    }

    override def toString(): String = s"${aggregate()}"
}

object Interval {
    @`inline`
    def noGreaterThan(x: Long, ceil: Long) = {
        if (x < ceil) x else ceil
    }

    @`inline`
    def noLessThan(x: Long, floor: Long) = {
        if (x > floor) x else floor
    }

    @`inline`
    def toLog10(x: Long) = {
        if (x < 1) 0
        else if (x < 10) 1
        else if (x < 100) 2
        else if (x < 1000) 3
        else if (x < 10000) 4
        else if (x < 100000) 5
        else if (x < 1000000) 6
        else 7
    }
}