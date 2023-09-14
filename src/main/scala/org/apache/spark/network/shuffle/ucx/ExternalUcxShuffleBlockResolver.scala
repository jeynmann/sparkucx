package org.apache.spark.network.shuffle.ucx

import java.io._
import java.nio.charset.StandardCharsets
import java.util._
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.regex.Matcher
import java.util.regex.Pattern

import scala.collection.mutable

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.annotations.VisibleForTesting
import com.google.common.base.Objects
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.google.common.cache.Weigher
import com.google.common.collect.Maps
import org.iq80.leveldb.DB
import org.iq80.leveldb.DBIterator
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.apache.spark.network.buffer.FileSegmentManagedBuffer
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo
import org.apache.spark.network.util.LevelDBProvider
import org.apache.spark.network.util.LevelDBProvider.StoreVersion
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.network.util.NettyUtils
import org.apache.spark.network.util.TransportConf
import org.apache.spark.network.shuffle.ExternalShuffleBlockResolver
import org.apache.spark.network.shuffle.ShuffleIndexInformation

import org.apache.spark.network.yarn.ucx.UcxLogging

class ExternalUcxShuffleBlockResolver(conf: TransportConf, registeredExecutorFile: File)
  extends ExternalShuffleBlockResolver(conf, registeredExecutorFile) with UcxLogging {
  type AppExecId = ExternalShuffleBlockResolver.AppExecId

  var executors: ConcurrentMap[AppExecId, ExecutorShuffleInfo] = _
  var shuffleIndexCache: LoadingCache[File, ShuffleIndexInformation] = _
  var directoryCleaner: Executor = _
  var db: DB = _
  val knownManagers = mutable.Set(
    "org.apache.spark.shuffle.sort.SortShuffleManager",
    "org.apache.spark.shuffle.unsafe.UnsafeShuffleManager",
    "org.apache.spark.shuffle.ucx.ExternalUcxShuffleManager")

  init()

  protected def init(): Unit = {
    val indexCacheSize = conf.get("spark.shuffle.service.index.cache.size", "100m")
    val indexCacheLoader =
        new CacheLoader[File, ShuffleIndexInformation]() {
          def load(file: File): ShuffleIndexInformation = {
            return new ShuffleIndexInformation(file)
          }
        }
    shuffleIndexCache = CacheBuilder.newBuilder()
      .maximumWeight(JavaUtils.byteStringAsBytes(indexCacheSize))
      .weigher(new Weigher[File, ShuffleIndexInformation]() {
        def weigh(file: File, indexInfo: ShuffleIndexInformation): Int = {
          indexInfo.getSize()
        }
      })
      .build(indexCacheLoader)
    db = LevelDBProvider.initLevelDB(registeredExecutorFile,
      ExternalUcxShuffleBlockResolver.CURRENT_VERSION,
      ExternalUcxShuffleBlockResolver.mapper)
    if (db != null) {
      executors = ExternalUcxShuffleBlockResolver.reloadRegisteredExecutors(db)
    } else {
      executors = Maps.newConcurrentMap()
    }
    directoryCleaner = Executors.newSingleThreadExecutor(
      NettyUtils.createThreadFactory("sparkucx-shuffle-directory-cleaner"))
  }

  override def getRegisteredExecutorsSize(): Int = executors.size

  /** Registers a new Executor with all the configuration we need to find its shuffle files. */
  def registerExecutor(
      appId: String,
      execId: String,
      executorInfo: ExecutorShuffleInfo): Unit = {
    val fullId = new AppExecId(appId, execId)
    logInfo("Registered executor ${fullId} with ${executorInfo}")
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

  /**
   * Obtains a FileSegmentManagedBuffer from (shuffleId, mapId, reduceId). We make assumptions
   * about how the hash and sort based shuffles store their data.
   */
  def getBlockData(
      appId: String,
      execId: String,
      shuffleId: Int,
      mapId: Int,
      reduceId: Int):ManagedBuffer = {
    val executor = executors.get(new AppExecId(appId, execId))
    if (executor == null) {
      throw new RuntimeException(
        String.format("Executor is not registered (appId=%s, execId=%s)", appId, execId))
    }
    getSortBasedShuffleBlockData(executor, shuffleId, mapId, reduceId)
  }

  /**
   * Removes our metadata of all executors registered for the given application, and optionally
   * also deletes the local directories associated with the executors of that application in a
   * separate thread.
   *
   * It is not valid to call registerExecutor() for an executor with this appId after invoking
   * this method.
   */
  def applicationRemoved(appId: String, cleanupLocalDirs: Boolean): Unit = {
    logInfo(s"Application ${appId} removed, cleanupLocalDirs = ${cleanupLocalDirs}")
    val it = executors.entrySet().iterator()
    while (it.hasNext()) {
      val entry = it.next()
      val fullId = entry.getKey()
      val executor = entry.getValue()

      // Only touch executors associated with the appId that was removed.
      if (appId.equals(fullId.appId)) {
        it.remove()
        if (db != null) {
          try {
            db.delete(ExternalUcxShuffleBlockResolver.dbAppExecKey(fullId))
          } catch {
            case e: IOException => logError(s"Error deleting ${appId} from executor state db", e)
          }
        }

        if (cleanupLocalDirs) {
          logInfo(s"Cleaning up executor ${fullId}'s ${executor.localDirs.length} local dirs")

          // Execute the actual deletion in a different thread, as it may take some time.
          directoryCleaner.execute(() => deleteExecutorDirs(executor.localDirs))
        }
      }
    }
  }

  /**
   * Removes all the non-shuffle files in any local directories associated with the finished
   * executor.
   */
  def executorRemoved(executorId: String, appId: String): Unit = {
    logInfo(s"Clean up non-shuffle files associated with the finished executor ${executorId}")
    val fullId = new AppExecId(appId, executorId)
    val executor = executors.get(fullId)
    if (executor == null) {
      // Executor not registered, skip clean up of the local directories.
      logInfo(s"Executor is not registered (appId=${appId}, execId=${executorId})")
    } else {
      logInfo(s"Cleaning up non-shuffle files in executor ${fullId}'s ${executor.localDirs.length} local dirs")

      // Execute the actual deletion in a different thread, as it may take some time.
      directoryCleaner.execute(() => deleteNonShuffleFiles(executor.localDirs))
    }
  }

  /**
   * Synchronously deletes each directory one at a time.
   * Should be executed in its own thread, as this may take a long time.
   */
  private def deleteExecutorDirs(dirs: Array[String]): Unit = {
    for (localDir <- dirs) {
      try {
        JavaUtils.deleteRecursively(new File(localDir))
        logDebug(s"Successfully cleaned up directory: ${localDir}")
      } catch {
        case e: Exception => logError("Failed to delete directory: " + localDir, e)
      }
    }
  }

  /**
   * Synchronously deletes non-shuffle files in each directory recursively.
   * Should be executed in its own thread, as this may take a long time.
   */
  private def deleteNonShuffleFiles(dirs: Array[String]): Unit = {
    val filter = new FilenameFilter() {
      override def accept(dir: File, name: String): Boolean = {
        // Don't delete shuffle data or shuffle index files.
        !name.endsWith(".index") && !name.endsWith(".data")
      }
    }

    for (localDir <- dirs) {
      try {
        JavaUtils.deleteRecursively(new File(localDir), filter)
        logDebug(s"Successfully cleaned up non-shuffle files in directory: ${localDir}")
      } catch {
        case e: Exception => logError("Failed to delete non-shuffle files in directory: " + localDir, e)
      }
    }
  }

  /**
   * Sort-based shuffle data uses an index called "shuffle_ShuffleId_MapId_0.index" into a data file
   * called "shuffle_ShuffleId_MapId_0.data". This logic is from IndexShuffleBlockResolver,
   * and the block id format is from ShuffleDataBlockId and ShuffleIndexBlockId.
   */
  private def getSortBasedShuffleBlockData(
    executor: ExecutorShuffleInfo, shuffleId: Int, mapId: Int, reduceId: Int): ManagedBuffer = {
    val indexFile = ExternalUcxShuffleBlockResolver.getFile(executor.localDirs, executor.subDirsPerLocalDir,
      "shuffle_" + shuffleId + "_" + mapId + "_0.index")

    try {
      val shuffleIndexInformation = shuffleIndexCache.get(indexFile)
      val shuffleIndexRecord = shuffleIndexInformation.getIndex(reduceId)
      return new FileSegmentManagedBuffer(
        conf,
        ExternalUcxShuffleBlockResolver.getFile(executor.localDirs, executor.subDirsPerLocalDir,
          "shuffle_" + shuffleId + "_" + mapId + "_0.data"),
        shuffleIndexRecord.getOffset(),
        shuffleIndexRecord.getLength())
    } catch {
      case e: ExecutionException => throw new RuntimeException("Failed to open file: " + indexFile, e)
    }
  }

  def close(): Unit = {
    if (db != null) {
      try {
        db.close()
      } catch {
        case e: IOException => logError("Exception closing leveldb with registered executors", e)
      }
    }
  }
}

object ExternalUcxShuffleBlockResolver {
  type AppExecId = ExternalShuffleBlockResolver.AppExecId

  val mapper = new ObjectMapper()
  val APP_KEY_PREFIX = "AppExecShuffleInfo"
  val CURRENT_VERSION = new StoreVersion(1, 0)
  val MULTIPLE_SEPARATORS = Pattern.compile(File.separator + "{2,}")

  /**
   * Hashes a filename into the corresponding local directory, in a manner consistent with
   * Spark's DiskBlockManager.getFile().
   */
  def getFile(localDirs: Array[String], subDirsPerLocalDir: Int, filename: String): File = {
    val hash = JavaUtils.nonNegativeHash(filename)
    val localDir = localDirs(hash % localDirs.length)
    val subDirId = (hash / localDirs.length) % subDirsPerLocalDir
    new File(createNormalizedInternedPathname(
        localDir, f"$subDirId%02x", filename))
  }

  /**
   * This method is needed to avoid the situation when multiple File instances for the
   * same pathname "foo/bar" are created, each with a separate copy of the "foo/bar" String.
   * According to measurements, in some scenarios such duplicate strings may waste a lot
   * of memory (~ 10% of the heap). To avoid that, we intern the pathname, and before that
   * we make sure that it's in a normalized form (contains no "//", "///" etc.) Otherwise,
   * the internal code in java.io.File would normalize it later, creating a new "foo/bar"
   * String copy. Unfortunately, we cannot just reuse the normalization code that java.io.File
   * uses, since it is in the package-private class java.io.FileSystem.
   */
  def createNormalizedInternedPathname(dir1: String, dir2: String, fname: String): String = {
    var pathname = dir1 + File.separator + dir2 + File.separator + fname
    val m = MULTIPLE_SEPARATORS.matcher(pathname)
    pathname = m.replaceAll("/")
    // A single trailing slash needs to be taken care of separately
    if (pathname.length() > 1 && pathname.endsWith("/")) {
      pathname = pathname.substring(0, pathname.length() - 1)
    }
    pathname.intern()
  }

  def dbAppExecKey(appExecId: AppExecId) = {
    // we stick a common prefix on all the keys so we can find them in the DB
    val appExecJson = mapper.writeValueAsString(appExecId)
    val key = (APP_KEY_PREFIX + "" + appExecJson)
    key.getBytes(StandardCharsets.UTF_8)
  }

  def parseDbAppExecKey(s: String): AppExecId = {
    if (!s.startsWith(APP_KEY_PREFIX)) {
      throw new IllegalArgumentException("expected a string starting with " + APP_KEY_PREFIX)
    }
    val json = s.substring(APP_KEY_PREFIX.length() + 1)
    mapper.readValue(json, classOf[AppExecId])
  }

  def reloadRegisteredExecutors(db: DB):
    ConcurrentMap[AppExecId, ExecutorShuffleInfo] = {
    val registeredExecutors = Maps.newConcurrentMap[AppExecId, ExecutorShuffleInfo]()
    if (db != null) {
      val itr = db.iterator()
      itr.seek(APP_KEY_PREFIX.getBytes(StandardCharsets.UTF_8))
      while (itr.hasNext()) {
        val e = itr.next()
        val key = new String(e.getKey(), StandardCharsets.UTF_8)
        if (!key.startsWith(APP_KEY_PREFIX)) {
          return registeredExecutors
        }
        val id = parseDbAppExecKey(key)
        UcxLogging.log_.info("Reloading registered executors: " +  id.toString())
        val shuffleInfo = mapper.readValue(e.getValue(), classOf[ExecutorShuffleInfo])
        registeredExecutors.put(id, shuffleInfo)
      }
    }
    registeredExecutors
  }
}
