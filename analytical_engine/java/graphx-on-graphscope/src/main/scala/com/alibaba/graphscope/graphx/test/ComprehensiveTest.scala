package com.alibaba.graphscope.graphx.test

import com.alibaba.graphscope.graphx.shuffle.EdgeShuffle
import org.apache.spark.graphx.Graph
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object ComprehensiveTest extends Logging{
  def main(array: Array[String]) : Unit = {
    require(array.length == 3)
    val fileName = array(0)
    val partNum = array(1).toInt
    val engine = array(2)
    require(engine.equals("gs") || engine.equals("graphx"))
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.getConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sc.getConf.registerKryoClasses(Array(classOf[EdgeShuffle[_,_]], classOf[Array[Long]], classOf[Array[Int]]))

    def run(graph : Graph[Long,Long]) : Unit = {
      val time0 = System.nanoTime()


    }

  }

}
