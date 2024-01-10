/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

object ExternalUcxAmId {
    // client -> server
    final val ADDRESS = 0
    final val CONNECT = 1
    final val FETCH_BLOCK = 2
    final val FETCH_STREAM = 3
    // server -> client
    final val REPLY_ADDRESS = 0
    final val REPLY_SLICE = 1
    final val REPLY_BLOCK = 2
    final val REPLY_STREAM = 3
}