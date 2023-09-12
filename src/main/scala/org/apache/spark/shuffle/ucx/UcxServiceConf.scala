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
class UcxServiceConf(conf: Configuration) extends UcxShuffleConf(new SparkConf) {

  def getHadoopConf: Configuration = conf

  lazy val ucxServicePort: Int = conf.getInt(UcxServiceConf.SPARK_UCX_SHUFFLE_SERVICE_PORT_KEY,
                                                  UcxServiceConf.SPARK_UCX_SHUFFLE_SERVICE_PORT_DEFAULT)
}

object UcxServiceConf {
  val SPARK_UCX_SHUFFLE_SERVICE_PORT_KEY = "spark.shuffle.ucx.service.port"
  val SPARK_UCX_SHUFFLE_SERVICE_PORT_DEFAULT = 3338
}