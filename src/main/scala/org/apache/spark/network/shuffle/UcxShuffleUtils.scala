package org.apache.spark.network.shuffle

object UcxShuffleUtils {
    def getManagerFromHandler(blockHandler: ExternalShuffleBlockHandler) = blockHandler.blockManager
}