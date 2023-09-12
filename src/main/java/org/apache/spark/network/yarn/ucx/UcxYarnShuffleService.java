/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
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

package org.apache.spark.network.yarn.ucx;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.api.*;
import org.apache.spark.network.util.LevelDBProvider;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.crypto.AuthServerBootstrap;
import org.apache.spark.network.sasl.ShuffleSecretManager;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler;
import org.apache.spark.network.shuffle.ExternalShuffleBlockResolver;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.yarn.YarnShuffleService;
import org.apache.spark.network.yarn.util.HadoopConfigProvider;

import org.apache.spark.shuffle.ucx.UcxServiceConf;
import org.apache.spark.shuffle.ucx.UcxShuffleTransportServer;

public class UcxYarnShuffleService extends YarnShuffleService {
  static final Logger logger = LoggerFactory.getLogger(YarnShuffleService.class);

  UcxShuffleTransportServer ucxTransport;
  ExternalShuffleBlockResolver blockManager;

  public UcxYarnShuffleService() {
    super();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    blockManager = blockHandler.blockManager;

    logger.info("Start launching UcxShuffleTransportServer");
    UcxServiceConf ucxConf = new UcxServiceConf(conf);
    ucxTransport = new UcxShuffleTransportServer(ucxConf, blockManager);
    ucxTransport.init();
  }

  @Override
  public void stopApplication(ApplicationTerminationContext context) {
    super.stopApplication(context);
    // ucxTransport.applicationRemoved(appId, false /* clean up local dirs */);
  }

  @Override
  protected void serviceStop() {
    try {
      super.serviceStop();
      if (ucxTransport != null) {
          ucxTransport.close();
          ucxTransport = null;
      }
    } catch (Exception e) {
      logger.error("Exception when stopping service", e);
    }
  }
}
