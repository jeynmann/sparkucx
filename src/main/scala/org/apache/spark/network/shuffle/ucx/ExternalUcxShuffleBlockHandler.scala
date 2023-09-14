package org.apache.spark.network.shuffle.ucx

import java.io.File
import java.io.IOException
import java.nio.ByteBuffer
import java.util.HashMap
import java.util.Iterator
import java.util.Map

import com.codahale.metrics.Gauge
import com.codahale.metrics.Meter
import com.codahale.metrics.Metric
import com.codahale.metrics.MetricSet
import com.codahale.metrics.Timer
import com.google.common.annotations.VisibleForTesting
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.RpcResponseCallback
import org.apache.spark.network.client.TransportClient
import org.apache.spark.network.server.OneForOneStreamManager
import org.apache.spark.network.server.RpcHandler
import org.apache.spark.network.server.StreamManager
import org.apache.spark.network.shuffle.ExternalShuffleBlockResolver.AppExecId
import org.apache.spark.network.shuffle.protocol._
import org.apache.spark.network.util.NettyUtils
import org.apache.spark.network.util.TransportConf
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler

// class ExternalUcxShuffleBlockHandler extends ExternalShuffleBlockHandler {
// }