package org.apache.spark.graphx.test

import com.alibaba.graphscope.graphx.GraphScopeHelper
import org.apache.spark.graphx.rdd.GraphScopeRDD
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object WriteBackTest extends Logging{
    def main(array: Array[String]) : Unit = {
        require(array.length == 1)
        val hostIdsStr = array(0)
        val spark = SparkSession
          .builder
          .appName(s"${this.getClass.getSimpleName}")
          .getOrCreate()
        val sc = spark.sparkContext

      val graph = GraphScopeRDD.loadFragmentAsGraph(sc, hostIdsStr,"gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>");
        val hostAndIds = GraphScopeHelper.writeBackFragment(graph)
        log.info(s"Write back to GraphScope ${hostAndIds.mkString(",")}")

    //FIXME: Run GraphScope SSSP on this graph

    sc.stop()
    }
}
