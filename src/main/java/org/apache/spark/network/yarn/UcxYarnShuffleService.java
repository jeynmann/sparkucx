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

package org.apache.spark.network.yarn;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.api.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.crypto.AuthServerBootstrap;
import org.apache.spark.network.sasl.ShuffleSecretManager;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.yarn.util.HadoopConfigProvider;
import org.apache.spark.network.yarn.YarnShuffleService.AppId;

import org.apache.spark.network.shuffle.UcxLogging;
import org.apache.spark.network.shuffle.ExternalUcxShuffleBlockHandler;
import org.apache.spark.shuffle.ucx.ExternalUcxServerConf;
import org.apache.spark.shuffle.ucx.UcxShuffleTransportServer;

class JUcxYarnShuffleService extends YarnShuffleService {
  private static final Logger logger = LoggerFactory.getLogger(UcxYarnShuffleService.class);

  UcxShuffleTransportServer ucxTransport;

  // Port on which the shuffle server listens for fetch requests
  static final String SPARK_SHUFFLE_SERVICE_PORT_KEY = "spark.shuffle.service.port";
  static final int DEFAULT_SPARK_SHUFFLE_SERVICE_PORT = 7337;

  // Whether the shuffle server should authenticate fetch requests
  static final String SPARK_AUTHENTICATE_KEY = "spark.authenticate";
  static final boolean DEFAULT_SPARK_AUTHENTICATE = false;

  static final String RECOVERY_FILE_NAME = "registeredExecutors.ldb";
  static final String SECRETS_RECOVERY_FILE_NAME = "sparkShuffleRecovery.ldb";

  // Whether failure during service initialization should stop the NM.
  // static final String STOP_ON_FAILURE_KEY = "spark.yarn.shuffle.stopOnFailure";
  static final boolean DEFAULT_STOP_ON_FAILURE = false;

  public JUcxYarnShuffleService() {
    super();
  }
  /**
   * Start the shuffle server with the given configuration.
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    Class clazz = Class.forName("org.apache.spark.network.yarn.YarnShuffleService");
    // Ucx reflect private filed: _conf
    Field confFiled = clazz.getDeclaredField("_conf");
    confFiled.setAccessible(true);
    confFiled.set(this, conf);

    boolean stopOnFailure = conf.getBoolean(STOP_ON_FAILURE_KEY, DEFAULT_STOP_ON_FAILURE);

    try {
      // In case this NM was killed while there were running spark applications, we need to restore
      // lost state for the existing executors. We look for an existing file in the NM's local dirs.
      // If we don't find one, then we choose a file to use to save the state next time.  Even if
      // an application was stopped while the NM was down, we expect yarn to call stopApplication()
      // when it comes back
      if (_recoveryPath != null) {
        registeredExecutorFile = initRecoveryDb(RECOVERY_FILE_NAME);
      }

      TransportConf transportConf = new TransportConf("shuffle", new HadoopConfigProvider(conf));

      // Ucx RPC handler
      ExternalUcxShuffleBlockHandler ucxHandler = new ExternalUcxShuffleBlockHandler(transportConf, registeredExecutorFile);
      blockHandler = ucxHandler;

      // If authentication is enabled, set up the shuffle server to use a
      // special RPC handler that filters out unauthenticated fetch requests
      List<TransportServerBootstrap> bootstraps = Lists.newArrayList();
      boolean authEnabled = conf.getBoolean(SPARK_AUTHENTICATE_KEY, DEFAULT_SPARK_AUTHENTICATE);
      if (authEnabled) {
        secretManager = new ShuffleSecretManager();
        if (_recoveryPath != null) {
          // Reflect private method: loadSecretsFromDb
          Method loadSecretsFromDbMethod = clazz.getDeclaredMethod("loadSecretsFromDb");
          loadSecretsFromDbMethod.setAccessible(true);
          loadSecretsFromDbMethod.invoke(this);
        }
        bootstraps.add(new AuthServerBootstrap(transportConf, secretManager));
      }

      int port = conf.getInt(
        SPARK_SHUFFLE_SERVICE_PORT_KEY, DEFAULT_SPARK_SHUFFLE_SERVICE_PORT);
      TransportContext transportContext = new TransportContext(transportConf, blockHandler);
      // Field transportContextFiled = clazz.getDeclaredField("transportContext");
      // transportContextFiled.setAccessible(true);
      // transportContextFiled.set(this, transportContext);
      // Ucx Reflect shuffleServer
      Field shuffleServerField = clazz.getDeclaredField("shuffleServer");
      shuffleServerField.setAccessible(true);
      shuffleServerField.set(this, transportContext.createServer(port, bootstraps));
      // the port should normally be fixed, but for tests its useful to find an open port
      port = ((TransportServer)(shuffleServerField.get(this))).getPort();
      boundPort = port;
      String authEnabledString = authEnabled ? "enabled" : "not enabled";
      logger.info("Started YARN shuffle service for Spark on port {}. " +
        "Authentication is {}.  Registered executor file is {}", port, authEnabledString,
        registeredExecutorFile);

      // Ucx Transport
      logger.info("Start launching UcxShuffleTransportServer");
      ExternalUcxServerConf ucxConf = new ExternalUcxServerConf(conf);
      ucxTransport = new UcxShuffleTransportServer(ucxConf, ucxHandler.ucxBlockManager());
      ucxTransport.init();
    } catch (Exception e) {
      if (stopOnFailure) {
        throw e;
      } else {
        logger.error("Start UcxYarnShuffleService failed: ", e);
        // noteFailure(e)
      }
    }
  }

  @Override
  protected void serviceStop() {
    ucxTransport.close();
    super.serviceStop();
  }
}
