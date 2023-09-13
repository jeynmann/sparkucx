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

package org.apache.spark.network.yarn.ucx

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.api.{ApplicationInitializationContext, ApplicationTerminationContext}

import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler
import org.apache.spark.network.shuffle.ExternalShuffleBlockResolver
import org.apache.spark.network.yarn.YarnShuffleService

import org.apache.spark.network.yarn.UcxYarnUtils
import org.apache.spark.network.shuffle.UcxShuffleUtils
import org.apache.spark.shuffle.ucx.UcxServiceConf
import org.apache.spark.shuffle.ucx.UcxShuffleTransportServer

class UcxYarnShuffleService extends YarnShuffleService with UcxLogging {
  var sparkBlockHandler: ExternalShuffleBlockHandler = _
  var sparkBlockManager: ExternalShuffleBlockResolver = _
  var ucxTransport: UcxShuffleTransportServer = _

  protected override def serviceInit(conf: Configuration): Unit = {
    super.serviceInit(conf)
    sparkBlockHandler = UcxYarnUtils.getHandlerFromService(this.asInstanceOf[YarnShuffleService])
    sparkBlockManager = UcxShuffleUtils.getManagerFromHandler(sparkBlockHandler)

    logInfo("Start launching UcxShuffleTransportServer")
    val ucxConf = new UcxServiceConf(conf)
    ucxTransport = new UcxShuffleTransportServer(ucxConf, sparkBlockManager)
    ucxTransport.init()
  }

  override def stopApplication(context: ApplicationTerminationContext): Unit = {
    super.stopApplication(context)
    // ucxTransport.applicationRemoved(appId, false /* clean up local dirs */)
  }

  protected override def serviceStop(): Unit = {
    try {
      super.serviceStop()
      if (ucxTransport != null) {
          ucxTransport.close()
          ucxTransport = null
      }
    } catch {
      case e: Exception => logError("Exception when stopping service", e)
    }
  }
}
