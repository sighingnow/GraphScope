package com.alibaba.graphscope.example

import com.alibaba.graphscope.graphx.GraphLoader
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object Test extends Logging{
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
    if (args.length < 1) {
      println("Expect 1 args")
      return 0;
    }
    val eFilePath = args(0);
    log.info(s"Running for efile ${eFilePath}")
    val graph = GraphLoader.edgeListFile[Long, Long](sc, eFilePath, 1L)

   log.info("Finish running test")
  }
}
