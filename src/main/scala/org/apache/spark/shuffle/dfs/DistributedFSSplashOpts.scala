package org.apache.spark.shuffle.dfs

import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}

object DistributedFSSplashOpts {
  lazy val dfsFileSystem: ConfigEntry[String] =
    ConfigBuilder("spark.shuffle.splash.dfs.filesystem")
        .doc("absolute path of the hdfs host and folder for DFS shuffles")
        .stringConf
        .createWithDefault(null)
}
