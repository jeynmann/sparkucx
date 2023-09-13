package org.apache.spark.network.yarn

object UcxYarnUtils {
    def getHandlerFromService(service: YarnShuffleService) = service.blockHandler
}