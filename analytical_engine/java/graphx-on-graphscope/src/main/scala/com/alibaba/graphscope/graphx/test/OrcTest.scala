package com.alibaba.graphscope.graphx.test

import com.alibaba.graphscope.graphx.rdd.impl.VertexDataMessage
import com.alibaba.graphscope.graphx.shuffle.EdgeShuffle
import com.alibaba.graphscope.graphx.utils.DoubleDouble
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

class OrcTest extends Logging{
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.getConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sc.getConf.registerKryoClasses(Array(classOf[EdgeShuffle[_,_]], classOf[Array[Long]], classOf[Array[Int]], classOf[VertexDataMessage[_]], classOf[DoubleDouble]))

    if (args.length < 2) {
      println("Expect 2 args")
      return 0;
    }
    val filePath = args(0)
    val numPart = Integer.valueOf(args(1))

    val loaded = spark.read.option("inferSchema",true).orc(filePath)
  }
}
