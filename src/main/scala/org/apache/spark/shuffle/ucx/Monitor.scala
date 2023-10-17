
package org.apache.spark.shuffle.ucx

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.HashMap
import javax.swing.text.StyledEditorKit.UnderlineAction

class Wrap[T](var value: T){}

class Record[T, V] {
    type Value = T
    type Underlying = V
     val recordMap = new TrieMap[Long, V]
     val recordTls = new ThreadLocal[V] {
        override def initialValue = {
            recordMap.getOrElseUpdate(Thread.currentThread().getId(), initialize)
        }
    }

    def add(x: T): Unit = {}
    def local(): T = ???
    def aggregate(): T = ???
    def initialize(): V = ???
}

class LongRecord extends Record[Long, Wrap[Long]] {
    override def add(x: Long): Unit = recordTls.get.value += x
    override def local(): Long = recordTls.get.value
    override def aggregate(): Long = recordMap.map(_._2.value).sum
    override def initialize(): Wrap[Long] = new Wrap[Long](0)
}

class LongArrayRecord(val size: Int) extends Record[Array[Long], Array[Long]] {
    def update(x: (Int, Long)): Unit = {
        recordTls.get()(x._1) = x._2
        recordTls.get()(size) += 1
    }
    override def add(x: Array[Long]): Unit = {
        recordTls.get()(x(0).toInt) = x(1)
        recordTls.get()(size) += 1
    }
    override def local(): Array[Long] = recordTls.get()
    override def aggregate(): Array[Long] = {
        val real = recordMap.values.map(_(size).abs.min(size)).sum
        val num = size * recordMap.size
        val r = new Array[Long](num)
        var offset = 0
        recordMap.values.foreach {
            case (value) => if (offset < num) {
                for (i <- 0 until size) {
                    r(i + offset) = value(i)
                }
                offset += size
            }
        }
        r.sorted.slice(num - real.toInt.min(num), num)
    }
    override def initialize(): Array[Long] = new Array[Long](size + 1)
}

class Monitor[T] {
    def currentStats(currentTime: Long): T = ???
}

class BpsMonitor extends Monitor[Long] {
    val record = new LongRecord
    var timeStamp = System.currentTimeMillis()
    var dataStamp = 0L

    def add(x: Long): Unit = record.add(x)

    override def currentStats(currentTime: Long): Long = {
        val currentData = record.aggregate()
        val r = (currentData - dataStamp) / (currentTime - timeStamp).max(1) / 1000
        timeStamp = currentTime
        dataStamp = currentData
        r
    }

    override def toString(): String = s"${currentStats(System.currentTimeMillis())}"
}

class PsMonitor extends Monitor[Array[Long]] {
    val record = new LongArrayRecord(1000)
    var timeStamp = System.currentTimeMillis()
    var dataStamp = new Array[Long](5) // 50,90,99,999,M
    val id = new LongRecord

    def add(x: Long): Unit = {
        record.update((id.local().toInt.abs % 1000, x))
        id.add(1)
    }

    override def currentStats(currentTime: Long): Array[Long] = {
        val sorted = record.aggregate()
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

    override def toString(): String = s"${currentStats(System.currentTimeMillis()).toSeq}"
}
