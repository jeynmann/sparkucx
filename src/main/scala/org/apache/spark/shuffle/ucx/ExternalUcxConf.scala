/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

/**
 * Plugin configuration properties.
 */
trait ExternalUcxConf {
  lazy val preallocateBuffersMap: Map[Long, Int] = ExternalUcxConf.PREALLOCATE_BUFFERS_DEFAULT
  lazy val minBufferSize: Long = ExternalUcxConf.MIN_BUFFER_SIZE_DEFAULT
  lazy val minRegistrationSize: Int = ExternalUcxConf.MIN_REGISTRATION_SIZE_DEFAULT
  lazy val listenerAddress: String = ExternalUcxConf.SOCKADDR_DEFAULT
  lazy val useWakeup: Boolean = ExternalUcxConf.WAKEUP_FEATURE_DEFAULT
  lazy val numIoThreads: Int = ExternalUcxConf.NUM_IO_THREADS_DEFAULT
  lazy val numListenerThreads: Int = ExternalUcxConf.NUM_LISTNER_THREADS_DEFAULT
  lazy val numWorkers: Int = ExternalUcxConf.NUM_WORKERS_DEFAULT
  lazy val maxBlocksPerRequest: Int = ExternalUcxConf.MAX_BLOCKS_IN_FLIGHT_DEFAULT
  lazy val ucxServerPort: Int = ExternalUcxConf.SPARK_UCX_SHUFFLE_SERVICE_PORT_DEFAULT
}

object ExternalUcxConf {
  protected def getUcxConf(name: String) = s"spark.shuffle.ucx.$name"

  lazy val PREALLOCATE_BUFFERS_KEY = getUcxConf("memory.preAllocateBuffers")
  lazy val PREALLOCATE_BUFFERS_DEFAULT = Map.empty[Long, Int]

  lazy val MIN_BUFFER_SIZE_KEY = getUcxConf("memory.minBufferSize")
  lazy val MIN_BUFFER_SIZE_DEFAULT = 4096

  lazy val MIN_REGISTRATION_SIZE_KEY = getUcxConf("memory.minAllocationSize")
  lazy val MIN_REGISTRATION_SIZE_DEFAULT = 1024 * 1024

  lazy val SOCKADDR_KEY = getUcxConf("listener.sockaddr")
  lazy val SOCKADDR_DEFAULT = "0.0.0.0:0"

  lazy val WAKEUP_FEATURE_KEY = getUcxConf("useWakeup")
  lazy val WAKEUP_FEATURE_DEFAULT = true

  lazy val NUM_IO_THREADS_KEY = getUcxConf("numIoThreads")
  lazy val NUM_IO_THREADS_DEFAULT = 1

  lazy val NUM_LISTNER_THREADS_KEY = getUcxConf("numListenerThreads")
  lazy val NUM_LISTNER_THREADS_DEFAULT = 4

  lazy val NUM_WORKERS_KEY = getUcxConf("numClientWorkers")
  lazy val NUM_WORKERS_DEFAULT = 2

  lazy val MAX_BLOCKS_IN_FLIGHT_KEY = getUcxConf("maxBlocksPerRequest")
  lazy val MAX_BLOCKS_IN_FLIGHT_DEFAULT = 50

  lazy val SPARK_UCX_SHUFFLE_SERVICE_PORT_KEY = getUcxConf("service.port")
  lazy val SPARK_UCX_SHUFFLE_SERVICE_PORT_DEFAULT = 3338
}