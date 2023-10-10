package tech.mlsql.example.app

import streaming.core.StreamingApp

/**
 * 2019-03-20 WilliamZhu(allwefantasy@gmail.com)
 */
object LocalSparkServiceApp {

  def main(args: Array[String]): Unit = {
    StreamingApp.main(Array(
      "-streaming.master", "local[*]",
      "-streaming.name", "julong",
      "-streaming.rest", "true",
      "-streaming.thrift", "false",
      "-streaming.platform", "spark",
      "-streaming.spark.service", "true",
      "-streaming.job.cancel", "true",
      "-streaming.datalake.path", "./data/",
      "-streaming.driver.port", "9003",
      "-spark.hadoop.mapreduce.job.run-local", "true",
      "-streaming.udf.clzznames com.code.udf.MyFunctions","true",
      "-plugin.Peer com.code.mlsql.peers.Peer",
      "-plugin.GPStorage com.code.mlsql.distrdb.GreenplumStorage",
      "-plugin.ChartsPlugin", "com.code.mlsql.charts.ChartsPlugin"
    ) ++ args )
  }
}
