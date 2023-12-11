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
  override lazy val preallocateBuffersMap: Map[Long, Int] = yarnConf.get(
    ExternalUcxConf.PREALLOCATE_BUFFERS_KEY).split(",").withFilter(s => s.nonEmpty)
      .map(entry => entry.split(":") match {
        case Array(bufferSize, bufferCount) => (bufferSize.toLong, bufferCount.toInt)
      }).toMap

  override lazy val minBufferSize: Long = yarnConf.getLong(
    ExternalUcxConf.MIN_BUFFER_SIZE_KEY,
    ExternalUcxConf.MIN_BUFFER_SIZE_DEFAULT)

  override lazy val minRegistrationSize: Int = yarnConf.getInt(
    ExternalUcxConf.MIN_REGISTRATION_SIZE_KEY,
    ExternalUcxConf.MIN_REGISTRATION_SIZE_DEFAULT)

  override lazy val listenerAddress: String = yarnConf.get(
    ExternalUcxConf.SOCKADDR_KEY,
    ExternalUcxConf.SOCKADDR_DEFAULT)

  override lazy val useWakeup: Boolean = yarnConf.getBoolean(
    ExternalUcxConf.WAKEUP_FEATURE_KEY,
    ExternalUcxConf.WAKEUP_FEATURE_DEFAULT)

  override lazy val numListenerThreads: Int = yarnConf.getInt(
    ExternalUcxConf.NUM_LISTNER_THREADS_KEY,
    ExternalUcxConf.NUM_LISTNER_THREADS_DEFAULT)

  override lazy val ucxServerPort: Int = yarnConf.getInt(
    ExternalUcxConf.SPARK_UCX_SHUFFLE_SERVICE_PORT_KEY,
    ExternalUcxConf.SPARK_UCX_SHUFFLE_SERVICE_PORT_DEFAULT)

  override lazy val maxReplySize: Long = yarnConf.getLong(
    ExternalUcxConf.MAX_REPLY_SIZE_KEY,
    ExternalUcxConf.MAX_REPLY_SIZE_DEFAULT)

  lazy val ucxEpsNum: Int = yarnConf.getInt(
    ExternalUcxServerConf.SPARK_UCX_SHUFFLE_EPS_NUM_KEY,
    ExternalUcxServerConf.SPARK_UCX_SHUFFLE_EPS_NUM_DEFAULT)
}

object ExternalUcxServerConf extends ExternalUcxConf {
  lazy val SPARK_UCX_SHUFFLE_EPS_NUM_KEY = "spark.shuffle.ucx.eps.num"
  lazy val SPARK_UCX_SHUFFLE_EPS_NUM_DEFAULT = 1024

  // Copied from spark ExternalShuffleService.java

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