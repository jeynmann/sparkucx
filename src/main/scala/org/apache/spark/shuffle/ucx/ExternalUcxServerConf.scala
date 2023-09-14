/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.SparkConf

/**
 * Plugin configuration properties.
 */
class ExternalUcxServerConf(val yarnConf: Configuration) extends ExternalUcxConf {
  lazy val ucxServicePort: Int = yarnConf.getInt(
    ExternalUcxServerConf.SPARK_UCX_SHUFFLE_SERVICE_PORT_KEY,
    ExternalUcxServerConf.SPARK_UCX_SHUFFLE_SERVICE_PORT_DEFAULT)

  lazy val ucxEpsNum: Int = yarnConf.getInt(
    ExternalUcxServerConf.SPARK_UCX_SHUFFLE_EPS_NUM_KEY,
    ExternalUcxServerConf.SPARK_UCX_SHUFFLE_EPS_NUM_DEFAULT)
}

object ExternalUcxServerConf {
  lazy val SPARK_UCX_SHUFFLE_SERVICE_PORT_KEY = "spark.shuffle.ucx.service.port"
  lazy val SPARK_UCX_SHUFFLE_SERVICE_PORT_DEFAULT = 3338

  lazy val SPARK_UCX_SHUFFLE_EPS_NUM_KEY = "spark.shuffle.ucx.eps.num"
  lazy val SPARK_UCX_SHUFFLE_EPS_NUM_DEFAULT = 8
  // @C
  // Port on which the shuffle server listens for fetch requests
  lazy val SPARK_SHUFFLE_SERVICE_PORT_KEY = "spark.shuffle.service.port"
  lazy val DEFAULT_SPARK_SHUFFLE_SERVICE_PORT = 7337

  // Whether the shuffle server should authenticate fetch requests
  lazy val SPARK_AUTHENTICATE_KEY = "spark.authenticate"
  lazy val DEFAULT_SPARK_AUTHENTICATE = false

  lazy val RECOVERY_FILE_NAME = "registeredExecutors.ldb"
  lazy val SECRETS_RECOVERY_FILE_NAME = "sparkShuffleRecovery.ldb"

  // Whether failure during service initialization should stop the NM.
  lazy val STOP_ON_FAILURE_KEY = "spark.yarn.shuffle.stopOnFailure"
  lazy val DEFAULT_STOP_ON_FAILURE = false
}