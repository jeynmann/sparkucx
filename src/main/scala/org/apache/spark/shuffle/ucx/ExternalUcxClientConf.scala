/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.network.util.ByteUnit

/**
 * Plugin configuration properties.
 */
class ExternalUcxClientConf(val sparkConf: SparkConf) extends SparkConf with ExternalUcxConf {

  def getSparkConf: SparkConf = sparkConf

  // Memory Pool
  private lazy val PREALLOCATE_BUFFERS =
  ConfigBuilder(ExternalUcxConf.PREALLOCATE_BUFFERS_KEY)
    .doc("Comma separated list of buffer size : buffer count pairs to preallocate in memory pool. E.g. 4k:1000,16k:500")
    .stringConf.createWithDefault("")

  override lazy val preallocateBuffersMap: Map[Long, Int] = {
    sparkConf.get(PREALLOCATE_BUFFERS).split(",").withFilter(s => s.nonEmpty)
      .map(entry => entry.split(":") match {
        case Array(bufferSize, bufferCount) => (bufferSize.toLong, bufferCount.toInt)
      }).toMap
  }

  private lazy val MIN_BUFFER_SIZE = ConfigBuilder(ExternalUcxConf.MIN_BUFFER_SIZE_KEY)
    .doc("Minimal buffer size in memory pool.")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(4096)

  override lazy val minBufferSize: Long = sparkConf.getSizeAsBytes(MIN_BUFFER_SIZE.key,
    MIN_BUFFER_SIZE.defaultValueString)

  private lazy val MIN_REGISTRATION_SIZE =
    ConfigBuilder(ExternalUcxConf.MIN_REGISTRATION_SIZE_KEY)
    .doc("Minimal memory registration size in memory pool.")
    .bytesConf(ByteUnit.MiB)
    .createWithDefault(1)

  override lazy val minRegistrationSize: Int = sparkConf.getSizeAsBytes(MIN_REGISTRATION_SIZE.key,
    MIN_REGISTRATION_SIZE.defaultValueString).toInt

  private lazy val SOCKADDR =
    ConfigBuilder(ExternalUcxConf.SOCKADDR_KEY)
      .doc("Whether to use socket address to connect executors.")
      .stringConf
      .createWithDefault("0.0.0.0:0")

  override lazy val listenerAddress: String = sparkConf.get(SOCKADDR.key, SOCKADDR.defaultValueString)

  private lazy val WAKEUP_FEATURE =
    ConfigBuilder(ExternalUcxConf.WAKEUP_FEATURE_KEY)
      .doc("Whether to use busy polling for workers")
      .booleanConf
      .createWithDefault(true)

  override lazy val useWakeup: Boolean = sparkConf.getBoolean(WAKEUP_FEATURE.key, WAKEUP_FEATURE.defaultValue.get)

  private lazy val NUM_IO_THREADS= ConfigBuilder(ExternalUcxConf.NUM_IO_THREADS_KEY)
    .doc("Number of threads in io thread pool")
    .intConf
    .createWithDefault(1)

  override lazy val numIoThreads: Int = sparkConf.getInt(NUM_IO_THREADS.key, NUM_IO_THREADS.defaultValue.get)

  private lazy val NUM_LISTNER_THREADS= ConfigBuilder(ExternalUcxConf.NUM_LISTNER_THREADS_KEY)
    .doc("Number of threads in listener thread pool")
    .intConf
    .createWithDefault(3)

  override lazy val numListenerThreads: Int = sparkConf.getInt(NUM_LISTNER_THREADS.key, NUM_LISTNER_THREADS.defaultValue.get)

  private lazy val NUM_WORKERS = ConfigBuilder(ExternalUcxConf.NUM_WORKERS_KEY)
    .doc("Number of client workers")
    .intConf
    .createWithDefault(1)

  override lazy val numWorkers: Int = sparkConf.getInt(NUM_WORKERS.key, sparkConf.getInt("spark.executor.cores",
    NUM_WORKERS.defaultValue.get))

  private lazy val MAX_BLOCKS_IN_FLIGHT = ConfigBuilder(ExternalUcxConf.MAX_BLOCKS_IN_FLIGHT_KEY)
    .doc("Maximum number blocks per request")
    .intConf
    .createWithDefault(50)

  override lazy val maxBlocksPerRequest: Int = sparkConf.getInt(MAX_BLOCKS_IN_FLIGHT.key, MAX_BLOCKS_IN_FLIGHT.defaultValue.get)

  override lazy val ucxServerPort: Int = sparkConf.getInt(
    ExternalUcxConf.SPARK_UCX_SHUFFLE_SERVICE_PORT_KEY,
    ExternalUcxConf.SPARK_UCX_SHUFFLE_SERVICE_PORT_DEFAULT)
}
