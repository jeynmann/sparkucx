package org.apache.spark.network.shuffle

import java.io._
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.lang.reflect.{Method, Field}

import scala.collection.mutable

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo
import org.apache.spark.network.util.LevelDBProvider
import org.apache.spark.network.util.LevelDBProvider.StoreVersion
import org.apache.spark.network.util.TransportConf
import org.apache.spark.network.shuffle.ExternalShuffleBlockResolver.AppExecId

class ExternalUcxShuffleBlockResolver(conf: TransportConf, registeredExecutorFile: File)
  extends ExternalShuffleBlockResolver(conf, registeredExecutorFile) with UcxLogging {
  private[this] var dbAppExecKeyMethod: Method = _
  private[this] var mapperFiled: Field = _
  lazy val ucxMapper = new ObjectMapper

  val knownManagers = mutable.Set(
    "org.apache.spark.shuffle.sort.SortShuffleManager",
    "org.apache.spark.shuffle.unsafe.UnsafeShuffleManager",
    "org.apache.spark.shuffle.ExternalUcxShuffleManager")

  init()

  def init(): Unit = {
    val clazz = Class.forName("org.apache.spark.network.shuffle.ExternalShuffleBlockResolver")
    try {
      dbAppExecKeyMethod = clazz.getDeclaredMethod("dbAppExecKey", classOf[AppExecId])
      dbAppExecKeyMethod.setAccessible(true)
    } catch {
      case e: Exception => {
        logError(s"Get dbAppExecKey from ExternalUcxShuffleBlockResolver failed: $e")
      }
    }
  }

  def dbAppExecKey(fullId: AppExecId): Array[Byte] = {
    dbAppExecKeyMethod.invoke(this, fullId).asInstanceOf[Array[Byte]]
  }

  /** Registers a new Executor with all the configuration we need to find its shuffle files. */
  override def registerExecutor(
      appId: String,
      execId: String,
      executorInfo: ExecutorShuffleInfo): Unit = {
    val fullId = new AppExecId(appId, execId)
    logInfo(s"Registered executor ${fullId} with ${executorInfo}")
    if (!knownManagers.contains(executorInfo.shuffleManager)) {
      throw new UnsupportedOperationException(
        "Unsupported shuffle manager of executor: " + executorInfo)
    }
    try {
      if (db != null) {
        val key = dbAppExecKey(fullId)
        val value = ucxMapper.writeValueAsString(executorInfo).getBytes(StandardCharsets.UTF_8)
        logInfo(s"@D DB saved $key -> $value")
        db.put(key, value)
      }
    } catch {
      case e: Exception => logError("Error saving registered executors", e)
    }
    executors.put(fullId, executorInfo)
    logInfo(s"@D executors=$executors ucxMapper=${ucxMapper} key=${dbAppExecKey(fullId)}")
  }

  override def getBlockData(
      appId: String,
      execId: String,
      shuffleId: Int,
      mapId: Int,
      reduceId: Int): ManagedBuffer = {
    logInfo(s"@D ($appId,$execId) ($shuffleId,$mapId,$reduceId)")
    val id = new AppExecId(appId, execId);
    val tmp = executors.get(id)
    logInfo(s"@D $id exsit=${tmp}")
    if (tmp == null) {
      executors.forEach((x: AppExecId, y: ExecutorShuffleInfo) => logInfo(s"${x} -> ${y}"))
    }
    val ans = super.getBlockData(appId, execId, shuffleId, mapId, reduceId);
    ans
  }
}
