package org.apache.spark.network.shuffle

import java.io._
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executor
import java.util.concurrent.Executors

import scala.collection.mutable

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo
import org.apache.spark.network.util.LevelDBProvider
import org.apache.spark.network.util.LevelDBProvider.StoreVersion
import org.apache.spark.network.util.TransportConf
import org.apache.spark.network.shuffle.ExternalShuffleBlockResolver.AppExecId

class ExternalUcxShuffleBlockResolver(conf: TransportConf, registeredExecutorFile: File)
  extends ExternalShuffleBlockResolver(conf, registeredExecutorFile) with UcxLogging {
  val knownManagers = mutable.Set(
    "org.apache.spark.shuffle.sort.SortShuffleManager",
    "org.apache.spark.shuffle.unsafe.UnsafeShuffleManager",
    "org.apache.spark.shuffle.ExternalUcxShuffleManager")

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
        val key = ExternalUcxShuffleBlockResolver.dbAppExecKey(fullId)
        val value = ExternalUcxShuffleBlockResolver.mapper.writeValueAsString(executorInfo).getBytes(StandardCharsets.UTF_8)
        db.put(key, value)
      }
    } catch {
      case e: Exception => logError("Error saving registered executors", e)
    }
    executors.put(fullId, executorInfo)
  }

  override def getBlockData(
      appId: String,
      execId: String,
      shuffleId: Int,
      mapId: Int,
      reduceId: Int) = {
    val id = new AppExecId(appId, execId);
    val tmp = executors.get(id)
    logInfo(s"@D $id exsit=${tmp} mapper=${ExternalUcxShuffleBlockResolver.mapper} key=${ExternalUcxShuffleBlockResolver.dbAppExecKey(id)}")
    if (tmp == null) {
      executors.forEach((x: AppExecId, y: ExecutorShuffleInfo) => logInfo(s"${x} -> ${y}"))
    }
    val ans = super.getBlockData(appId, execId, shuffleId, mapId, reduceId);
    ans
  }
}

object ExternalUcxShuffleBlockResolver {
  val clazz = Class.forName("org.apache.spark.network.shuffle.ExternalShuffleBlockResolver")

  val dbAppExecKeyMethod = clazz.getDeclaredMethod("dbAppExecKey", classOf[AppExecId])
  dbAppExecKeyMethod.setAccessible(true)
  def dbAppExecKey(fullId: AppExecId) = dbAppExecKeyMethod.invoke(null, fullId).asInstanceOf[Array[Byte]]

  val mapperFiled = clazz.getDeclaredField("mapper")
  mapperFiled.setAccessible(true)
  val mapper = mapperFiled.get(null).asInstanceOf[ObjectMapper]
}
