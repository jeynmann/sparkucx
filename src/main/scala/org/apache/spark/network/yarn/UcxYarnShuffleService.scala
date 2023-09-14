/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.yarn

import java.io.File
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import java.util.List
import java.util.Map

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.annotations.VisibleForTesting
import com.google.common.base.Objects
import com.google.common.base.Preconditions
import com.google.common.collect.Lists
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.yarn.api.records.ContainerId
import org.apache.hadoop.yarn.server.api._
import org.apache.spark.network.util.LevelDBProvider
import org.iq80.leveldb.DB
import org.iq80.leveldb.DBIterator

import org.apache.spark.network.TransportContext
import org.apache.spark.network.crypto.AuthServerBootstrap
import org.apache.spark.network.sasl.ShuffleSecretManager
import org.apache.spark.network.server.TransportServer
import org.apache.spark.network.server.TransportServerBootstrap
import org.apache.spark.network.util.TransportConf
import org.apache.spark.network.yarn.util.HadoopConfigProvider
import org.apache.spark.network.yarn.YarnShuffleService.AppId

import org.apache.spark.network.shuffle.UcxLogging
import org.apache.spark.network.shuffle.ExternalUcxShuffleBlockHandler
import org.apache.spark.shuffle.ucx.ExternalUcxServerConf
import org.apache.spark.shuffle.ucx.UcxShuffleTransportServer

class UcxYarnShuffleService extends AuxiliaryService("spark_shuffle") with UcxLogging {
  var ucxTransport: UcxShuffleTransportServer = _
  // An entity that manages the shuffle secret per application
  // This is used only if authentication is enabled
  var secretManager: ShuffleSecretManager = _

  // The actual server that serves shuffle files
  var shuffleServer: TransportServer = _

  var _conf: Configuration = _

  // The recovery path used to shuffle service recovery
  var _recoveryPath: Path = _

  // Handles registering executors and opening shuffle blocks
  var blockHandler: ExternalUcxShuffleBlockHandler = _

  // Where to store & reload executor info for recovering state after an NM restart
  var registeredExecutorFile: File = _

  // Where to store & reload application secrets for recovering state after an NM restart
  var secretsFile: File = _

  var db: DB = _

  init()

  def init(): Unit = {
    logInfo("Initializing YARN shuffle service for Spark")
    UcxYarnShuffleService.instance = this
  }

  /**
   * Return whether authentication is enabled as specified by the configuration.
   * If so, fetch requests will fail unless the appropriate authentication secret
   * for the application is provided.
   */
  def isAuthenticationEnabled()  = {
    secretManager != null
  }

  /**
   * Start the shuffle server with the given configuration.
   */
  override protected def serviceInit(conf: Configuration) = {
    _conf = conf

    val stopOnFailure = conf.getBoolean(
      ExternalUcxServerConf.STOP_ON_FAILURE_KEY,
      ExternalUcxServerConf.DEFAULT_STOP_ON_FAILURE)

    try {
      // In case this NM was killed while there were running spark applications, we need to restore
      // lost state for the existing executors. We look for an existing file in the NM's local dirs.
      // If we don't find one, then we choose a file to use to save the state next time.  Even if
      // an application was stopped while the NM was down, we expect yarn to call stopApplication()
      // when it comes back
      if (_recoveryPath != null) {
        registeredExecutorFile = initRecoveryDb(ExternalUcxServerConf.RECOVERY_FILE_NAME)
      }

      val transportConf = new TransportConf("shuffle", new HadoopConfigProvider(conf))
      blockHandler = new ExternalUcxShuffleBlockHandler(transportConf, registeredExecutorFile)

      // If authentication is enabled, set up the shuffle server to use a
      // special RPC handler that filters out unauthenticated fetch requests
      val bootstraps = Lists.newArrayList[TransportServerBootstrap]()
      val authEnabled = conf.getBoolean(
        ExternalUcxServerConf.SPARK_AUTHENTICATE_KEY,
        ExternalUcxServerConf.DEFAULT_SPARK_AUTHENTICATE)
      if (authEnabled) {
        secretManager = new ShuffleSecretManager()
        if (_recoveryPath != null) {
          loadSecretsFromDb()
        }
        bootstraps.add(new AuthServerBootstrap(transportConf, secretManager))
      }

      val portConf = conf.getInt(
        ExternalUcxServerConf.SPARK_SHUFFLE_SERVICE_PORT_KEY,
        ExternalUcxServerConf.DEFAULT_SPARK_SHUFFLE_SERVICE_PORT)
      val transportContext = new TransportContext(transportConf, blockHandler)
      shuffleServer = transportContext.createServer(portConf, bootstraps)
      // the port should normally be fixed, but for tests its useful to find an open port
      val port = shuffleServer.getPort()
      UcxYarnShuffleService.boundPort = port
      val authEnabledString = if (authEnabled) "enabled" else "not enabled"
      logInfo(s"Started YARN shuffle service for Spark on port ${port}. " +
        s"Authentication is ${authEnabledString}.  Registered executor file is ${registeredExecutorFile}")

      logInfo("Start launching UcxShuffleTransportServer")
      val ucxConf = new ExternalUcxServerConf(conf)
      ucxTransport = new UcxShuffleTransportServer(ucxConf, blockHandler.blockManager)
      ucxTransport.init()
    } catch {
      case e: Exception => if (stopOnFailure) {
        throw e
      } else {
        // noteFailure(e)
      }
    }
  }

  def loadSecretsFromDb(): Unit = {
    secretsFile = initRecoveryDb(ExternalUcxServerConf.SECRETS_RECOVERY_FILE_NAME)

    // Make sure this is protected in case its not in the NM recovery dir
    val fs = FileSystem.getLocal(_conf)
    fs.mkdirs(new Path(secretsFile.getPath()), new FsPermission(448.toShort)) // 0700=448

    db = LevelDBProvider.initLevelDB(secretsFile, UcxYarnShuffleService.CURRENT_VERSION, UcxYarnShuffleService.mapper)
    logInfo("Recovery location is: " + secretsFile.getPath())
    if (db != null) {
      logInfo("Going to reload spark shuffle data")
      val itr = db.iterator()
      itr.seek(UcxYarnShuffleService.APP_CREDS_KEY_PREFIX.getBytes(StandardCharsets.UTF_8))
      while (itr.hasNext()) {
        val e = itr.next()
        val key = new String(e.getKey(), StandardCharsets.UTF_8)
        if (!key.startsWith(UcxYarnShuffleService.APP_CREDS_KEY_PREFIX)) {
          return
        }
        val id = UcxYarnShuffleService.parseDbAppKey(key)
        val secret = UcxYarnShuffleService.mapper.readValue(e.getValue(), classOf[ByteBuffer])
        logInfo("Reloading tokens for app: " + id)
        secretManager.registerApp(id, secret)
      }
    }
  }

  override def initializeApplication(context: ApplicationInitializationContext) = {
    val appId = context.getApplicationId().toString()
    try {
      val shuffleSecret = context.getApplicationDataForService()
      if (isAuthenticationEnabled()) {
        val fullId = new AppId(appId)
        if (db != null) {
          val key = UcxYarnShuffleService.dbAppKey(fullId)
          val value = UcxYarnShuffleService.mapper.writeValueAsString(shuffleSecret).getBytes(StandardCharsets.UTF_8)
          db.put(key, value)
        }
        secretManager.registerApp(appId, shuffleSecret)
      }
    } catch {
      case e: Exception => logError(s"Exception when initializing application ${appId}", e)
    }
  }

  override def stopApplication(context: ApplicationTerminationContext) = {
    val appId = context.getApplicationId().toString()
    try {
      if (isAuthenticationEnabled()) {
        val fullId = new AppId(appId)
        if (db != null) {
          try {
            db.delete(UcxYarnShuffleService.dbAppKey(fullId))
          } catch {
            case e: IOException => logError(s"Error deleting ${appId} from executor state db", e)
          }
        }
        secretManager.unregisterApp(appId)
      }
      blockHandler.applicationRemoved(appId, false /* clean up local dirs */)
    } catch {
      case e: Exception => logError(s"Exception when stopping application ${appId}", e)
    }
  }

  override def initializeContainer(context: ContainerInitializationContext) = {
    val containerId = context.getContainerId()
    logInfo(s"Initializing container ${containerId}")
  }

  override def stopContainer(context: ContainerTerminationContext) = {
    val containerId = context.getContainerId()
    logInfo(s"Stopping container ${containerId}")
  }

  /**
   * Close the shuffle server to clean up any associated state.
   */
  override protected def serviceStop() = {
    try {
      if (ucxTransport != null) {
          ucxTransport.close()
          ucxTransport = null
      }
      if (shuffleServer != null) {
        shuffleServer.close()
      }
      if (blockHandler != null) {
        blockHandler.close()
      }
      if (db != null) {
        db.close()
      }
    } catch {
      case e: Exception => logError("Exception when stopping service", e)
    }
  }

  // Not currently used
  override def getMetaData(): ByteBuffer = {
    return ByteBuffer.allocate(0)
  }

  /**
   * Set the recovery path for shuffle service recovery when NM is restarted. This will be call
   * by NM if NM recovery is enabled.
   */
  override def setRecoveryPath(recoveryPath: Path) = {
    _recoveryPath = recoveryPath
  }

  /**
   * Get the path specific to this auxiliary service to use for recovery.
   */
  protected def getRecoveryPath(fileName: String): Path = {
    return _recoveryPath
  }

  /**
   * Figure out the recovery path and handle moving the DB if YARN NM recovery gets enabled
   * and DB exists in the local dir of NM by old version of shuffle service.
   */
  protected def initRecoveryDb(dbName: String): File = {
    require(_recoveryPath != null, 
      "recovery path should not be null if NM recovery is enabled")

    val recoveryFile = new File(_recoveryPath.toUri().getPath(), dbName)
    if (recoveryFile.exists()) {
      return recoveryFile
    }

    // db doesn't exist in recovery path go check local dirs for it
    val localDirs = _conf.getTrimmedStrings("yarn.nodemanager.local-dirs")
    for (dir <- localDirs) {
      val f = new File(new Path(dir).toUri().getPath(), dbName)
      if (f.exists()) {
        // If the recovery path is set then either NM recovery is enabled or another recovery
        // DB has been initialized. If NM recovery is enabled and had set the recovery path
        // make sure to move all DBs to the recovery path from the old NM local dirs.
        // If another DB was initialized first just make sure all the DBs are in the same
        // location.
        val newLoc = new Path(_recoveryPath, dbName)
        val copyFrom = new Path(f.toURI())
        if (!newLoc.equals(copyFrom)) {
          logInfo("Moving " + copyFrom + " to: " + newLoc)
          try {
            // The move here needs to handle moving non-empty directories across NFS mounts
            val fs = FileSystem.getLocal(_conf)
            fs.rename(copyFrom, newLoc)
          } catch {
            // Fail to move recovery file to new path, just continue on with new DB location
            case e: Exception => logError(s"Failed to move recovery file ${dbName} to the path ${_recoveryPath.toString()}", e)
          }
        }
        return new File(newLoc.toUri().getPath())
      }
    }

    return new File(_recoveryPath.toUri().getPath(), dbName)
  }
}

object UcxYarnShuffleService {
  // just for testing when you want to find an open port
  var boundPort = -1
  val mapper = new ObjectMapper()
  val APP_CREDS_KEY_PREFIX = "AppCreds"
  val CURRENT_VERSION = new LevelDBProvider.StoreVersion(1, 0)
  var instance: UcxYarnShuffleService = _

  def parseDbAppKey(s: String) = {
    if (!s.startsWith(APP_CREDS_KEY_PREFIX)) {
      throw new IllegalArgumentException("expected a string starting with " + APP_CREDS_KEY_PREFIX)
    }
    val json = s.substring(APP_CREDS_KEY_PREFIX.length() + 1)
    val parsed = mapper.readValue(json, classOf[AppId])
    parsed.appId
  }

  def dbAppKey(appExecId: AppId) = {
    // we stick a common prefix on all the keys so we can find them in the DB
    val appExecJson = mapper.writeValueAsString(appExecId)
    val key = (APP_CREDS_KEY_PREFIX + ";" + appExecJson)
    key.getBytes(StandardCharsets.UTF_8)
  }
}