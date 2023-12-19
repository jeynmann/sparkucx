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

import com.google.common.collect.Lists
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.server.api._

import org.apache.spark.network.TransportContext
import org.apache.spark.network.crypto.AuthServerBootstrap
import org.apache.spark.network.sasl.ShuffleSecretManager
import org.apache.spark.network.server.TransportServer
import org.apache.spark.network.server.TransportServerBootstrap
import org.apache.spark.network.util.TransportConf
import org.apache.spark.network.yarn.util.HadoopConfigProvider

import org.apache.spark.shuffle.utils.UcxLogging
import org.apache.spark.network.shuffle.ExternalUcxShuffleBlockHandler
import org.apache.spark.shuffle.ucx.ExternalUcxServerConf
import org.apache.spark.shuffle.ucx.ExternalUcxServerTransport

class UcxYarnShuffleService extends YarnShuffleService() with UcxLogging {
  var ucxTransport: ExternalUcxServerTransport = _

  /**
   * Start the shuffle server with the given configuration.
   */
  override protected def serviceInit(conf: Configuration) = {
    val clazz = Class.forName("org.apache.spark.network.yarn.YarnShuffleService")
    // Ucx reflect private filed: _conf
    val confFiled = clazz.getDeclaredField("_conf")
    confFiled.setAccessible(true)
    confFiled.set(this, conf)

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

      // Ucx RPC handler
      val ucxHandler = new ExternalUcxShuffleBlockHandler(transportConf, registeredExecutorFile)
      blockHandler = ucxHandler

      // If authentication is enabled, set up the shuffle server to use a
      // special RPC handler that filters out unauthenticated fetch requests
      val bootstraps = Lists.newArrayList[TransportServerBootstrap]()
      val authEnabled = conf.getBoolean(
        ExternalUcxServerConf.SPARK_AUTHENTICATE_KEY,
        ExternalUcxServerConf.DEFAULT_SPARK_AUTHENTICATE)
      if (authEnabled) {
        secretManager = new ShuffleSecretManager()
        if (_recoveryPath != null) {
          // Reflect private method: loadSecretsFromDb
          val loadSecretsFromDbMethod = clazz.getDeclaredMethod("loadSecretsFromDb")
          loadSecretsFromDbMethod.setAccessible(true)
          loadSecretsFromDbMethod.invoke(this)
        }
        bootstraps.add(new AuthServerBootstrap(transportConf, secretManager))
      }

      val portConf = conf.getInt(
        ExternalUcxServerConf.SPARK_SHUFFLE_SERVICE_PORT_KEY,
        ExternalUcxServerConf.DEFAULT_SPARK_SHUFFLE_SERVICE_PORT)
      val transportContext = new TransportContext(transportConf, blockHandler)
      // Ucx Reflect shuffleServer
      val shuffleServerField = clazz.getDeclaredField("shuffleServer")
      shuffleServerField.setAccessible(true)
      shuffleServerField.set(this, transportContext.createServer(portConf, bootstraps))
      // the port should normally be fixed, but for tests its useful to find an open port
      val port = shuffleServerField.get(this).asInstanceOf[TransportServer].getPort()
      YarnShuffleService.boundPort = port
      val authEnabledString = if (authEnabled) "enabled" else "not enabled"
      logInfo(s"Started YARN shuffle service for Spark on port ${port}. " +
        s"Authentication is ${authEnabledString}.  Registered executor file is ${registeredExecutorFile}")

      // Ucx Transport
      logInfo("Start launching ExternalUcxServerTransport")
      val ucxConf = new ExternalUcxServerConf(conf)
      ucxTransport = new ExternalUcxServerTransport(ucxConf, ucxHandler.ucxBlockManager)
      ucxTransport.init()
      ucxHandler.setTransport(ucxTransport)
    } catch {
      case e: Exception => if (stopOnFailure) {
        throw e
      } else {
        logError(s"Start UcxYarnShuffleService failed: $e")
        // noteFailure(e)
      }
    }
  }

  override protected def serviceStop(): Unit = {
    ucxTransport.close()
    super.serviceStop()
  }
}
