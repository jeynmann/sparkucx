
package org.apache.spark.shuffle.ucx

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.HashMap

class Record[T](var value: T) {
    def add(x: T): Unit = {}
    def mul(x: T): Unit = {}
    def add(x: (Int, Int)): Unit = {}
    def mul(x: (Int, Int)): Unit = {}
}

class IntRecord extends Record[Int](0) {
    override def add(x: Int): Unit = value += x
    override def mul(x: Int): Unit = value *= x
}

class LongRecord extends Record[Long](0) {
    override def add(x: Long): Unit = value += x
    override def mul(x: Long): Unit = value *= x
}

class IntArrayRecord(size: Int) extends Record[Array[Int]](new Array[Int](size)) {
    override def add(x: (Int, Int)): Unit = value(x._1) += x._2
    override def mul(x: (Int, Int)): Unit = value(x._1) += x._2
}

class IntMapRecord extends Record[HashMap[Int,Int]](HashMap.empty[Int,Int]) {
    override def add(x: (Int, Int)): Unit = value.put(x._1, value.getOrElse(x._1, 0) + x._2)
    override def mul(x: (Int, Int)): Unit = value.put(x._1, value.getOrElse(x._1, 0) + x._2)
}

class Monitor[T] {
     val recordMap = new TrieMap[Record[T], Int]
     val recordTls = new ThreadLocal[Record[T]] {
        override def initialValue = initialLocal
    }

    def initialLocal(): Record[T] = ???
    def aggregate(): T = ???

    def add(x: T): Unit = recordTls.get.add(x)
    def mul(x: T): Unit = recordTls.get.mul(x)
    def add(x: (Int, Int)): Unit = recordTls.get.add(x)
    def mul(x: (Int, Int)): Unit = recordTls.get.mul(x)
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

class BpsMonitor extends LongMonitor {
    var timeStamp = System.currentTimeMillis()
    var dataStamp = 0L

    def currentStats(currentTime: Long): Long = {
        val currentData = recordMap.map{ case (k,v) => k.value }.sum
        val r = (currentData - dataStamp) / (currentTime - timeStamp).max(1)
        if (currentTime - timeStamp > 1000) {
            timeStamp = currentTime
            dataStamp = currentData
        }
        r
    }

    override def toString(): String = s"${currentStats(System.currentTimeMillis())}"
}

class PsMonitor extends IntArrayMonitor(100) {
    var timeStamp = System.currentTimeMillis()
    var dataStamp = new Array[Int](5) // 50,90,99,999,M
    val id = new IntMonitor

    def currentStats(currentTime: Long): Array[Int] = {
        val sorted = aggregate().sorted
        val num = sorted.size
        if (num > 1) {
            dataStamp(0) = sorted(num * 1 / 2)
            dataStamp(1) = sorted(num * 9 / 10)
            dataStamp(2) = sorted(num * 99 / 100)
            dataStamp(3) = sorted(num * 999 / 1000)
            dataStamp(4) = sorted(num - 1)
            timeStamp = currentTime
        }
        dataStamp
    }

    def append(x: Int): Unit = {
        val tls = id.recordTls.get
        recordTls.get.value((tls.value % 100).abs) = x
        tls.add(1)
    }

    override def aggregate(): Array[Int] = {
        val num = recordMap.size * 100
        val r = new Array[Int](num)
        var offset = 0
        recordMap.foreach {
            case (k,v) => if (offset < num) {
                for (i <- 0 until 100) {
                    r(i + offset) = k.value(i)
                }
                offset += 100
            }
        }
        r
    }

    override def toString(): String = s"${currentStats(System.currentTimeMillis()).toSeq}"
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