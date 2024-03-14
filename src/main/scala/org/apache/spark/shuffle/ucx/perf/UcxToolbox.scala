/*
 * Copyright (C) 2022, NVIDIA CORPORATION & AFFILIATES. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx.perf

import org.apache.spark.shuffle.ucx._
import org.apache.commons.cli.{GnuParser, HelpFormatter, Options}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockManagerId

import java.net.InetSocketAddress
import java.util.concurrent.CountDownLatch

object UcxToolbox extends App with Logging {

  case class FetchCallBack(blockId: UcxShuffleBlockId)
    extends OperationCallback {
    private var done = false;

    def isCompleted(): Boolean = done

    override def onComplete(result: OperationResult): Unit = {
        logInfo(s"Fetch $blockId done.")
        done = true
    }
  }

  case class PerfOptions(
    local: InetSocketAddress,
    remote: InetSocketAddress,
    blockIds: Seq[UcxShuffleBlockId],
    callbacks: Seq[FetchCallBack],
    executorId: Int,
    numThreads: Int)

  private val sparkConf = new SparkConf()

  private val HELP_OPTION = "h"
  private val REMOTE_OPTION = "r"
  private val LOCAL_OPTION = "l"
  private val BLOCK_ID_OPTION = "b"
  private val EXECUTOR_ID_OPTION = "e"
  private val THREAD_OPTION = "t"

  private def initOptions(): Options = {
    val options = new Options()
    options.addOption(HELP_OPTION, "help", false, "display help message")
    options.addOption(LOCAL_OPTION, "local", true, "local address of the listener on the remote host")
    options.addOption(REMOTE_OPTION, "remote", true, "remote address of the listener on the remote host")
    options.addOption(BLOCK_ID_OPTION, "block-id", false, "block ID. format: (Appstring,shuffle id,map id,reduce id)")
    options.addOption(EXECUTOR_ID_OPTION, "executor-id", true, "executor ID")
    options.addOption(THREAD_OPTION, "thread", true, "Number of threads. Default: 1")
    options
  }

  private def parseOptions(args: Array[String]): PerfOptions = {
    val parser = new GnuParser()
    val options = initOptions()
    val cmd = parser.parse(options, args)

    if (cmd.hasOption(HELP_OPTION)) {
      new HelpFormatter().printHelp("UcxShufflePerfTool", options)
      System.exit(0)
    }

    val localAddress = if (cmd.hasOption(LOCAL_OPTION)) {
      val Array(host, port) = cmd.getOptionValue(LOCAL_OPTION).split(":")
      new InetSocketAddress(host, Integer.parseInt(port))
    } else {
      null
    }

    val remoteAddress = if (cmd.hasOption(REMOTE_OPTION)) {
      val Array(host, port) = cmd.getOptionValue(REMOTE_OPTION).split(":")
      new InetSocketAddress(host, Integer.parseInt(port))
    } else {
      null
    }

    val blockId = if (cmd.hasOption(BLOCK_ID_OPTION)) {
      val Array(shuffle, map, reduce) = cmd.getOptionValue(BLOCK_ID_OPTION).split(",")
      UcxShuffleBlockId(shuffle.toInt, map.toInt, reduce.toInt)
    } else {
      UcxShuffleBlockId(0,0,0)
    }
    val blockIds = Seq(blockId, blockId, blockId, blockId)
    val callbacks = blockIds.map(new FetchCallBack(_))

    PerfOptions(
      localAddress,
      remoteAddress,
      blockIds,
      callbacks,
      Integer.parseInt(cmd.getOptionValue(EXECUTOR_ID_OPTION, "0")),
      Integer.parseInt(cmd.getOptionValue(THREAD_OPTION, "1")))
  }

  def startClient(options: PerfOptions): Unit = {
    sparkConf.set("spark.app.id", s"ucxtest_${System.nanoTime}_0001")
    sparkConf.set("spark.shuffle.ucx.service.port", options.local.getPort.toString)
    sparkConf.set("spark.executor.cores", options.numThreads.toString)

    val localHost = options.local.getAddress.getHostAddress
    val localPort = options.local.getPort
    val managerId = BlockManagerId(options.executorId.toString, localHost, localPort,
                                   Option.empty[String])
    val ucxTransport = new ExternalUcxClientTransport(
        new ExternalUcxClientConf(sparkConf), managerId)

    ucxTransport.init()

    val remoteHost = options.remote.getAddress.getHostAddress
    val remotePort = options.remote.getPort
    ucxTransport.connect(remoteHost, Seq(remotePort))

    ucxTransport.fetchBlocksByBlockIds(remoteHost, options.executorId,
                                       options.blockIds, options.callbacks)

    while (!options.callbacks.forall(_.isCompleted)) {
        Thread.`yield`()
    }

    ucxTransport.close()
  }

  def start(): Unit = {
    val perfOptions = parseOptions(args)

    if (perfOptions.remote != null) {
      startClient(perfOptions)
    }
  }

  // LD_LIBRARY_PATH=${ucx_install}/lib/ \
  // UCX_LOG_LEVEL=info \
  // ./spark/bin/spark-submit --master local \
  // --class org.apache.spark.shuffle.ucx.perf.UcxToolbox \
  // target/ucx-spark-1.1-for-spark-2.4-jar-with-dependencies.jar
  start()
}
