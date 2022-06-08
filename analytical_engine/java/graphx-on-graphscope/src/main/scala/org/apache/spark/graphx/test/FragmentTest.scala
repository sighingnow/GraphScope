package org.apache.spark.graphx.test

import com.alibaba.graphscope.graphx.{GSSession, GraphScopeHelper}
import org.apache.spark.graphx.impl.GrapeGraphImpl
import org.apache.spark.graphx.rdd.GraphScopeRDD
import org.apache.spark.graphx.rdd.GraphScopeRDD.makeRDD
import org.apache.spark.graphx.test.FragmentAsRDDTest.log
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.TaskLocation
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import java.net.InetAddress
import scala.collection.mutable

object FragmentTest extends Logging{
  def main(array: Array[String]) : Unit = {
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    val gsSession: GSSession = GraphScopeHelper.createSession(sc)
    val graph: GrapeGraphImpl[Long, Long] = gsSession.run("import graphscope\n" +
      "sess = graphscope.session(cluster_type=\"hosts\",hosts=[\"d50\"], num_workers=1)\n" +
      "graph = sess.g(directed=True)\n" +
      "graph = graph.add_vertices(\"/home/graphscope/data/gstest/property/p2p-31_property_v_0\",\"person\")\n" +
      "graph = graph.add_edges(\"/home/graphscope/data/gstest/property/p2p-31_property_e_0\",label=\"knows\",src_label=\"person\",dst_label=\"person\")\n" +
      "graph_proj = graph.project(vertices={\"person\":[\"weight\"]}, edges={\"knows\" : [\"dist\"]})\n" +
      "simple = graph_proj._project_to_simple()\n" +
      "\"res_str:\" + simple.template_str + \";\" + simple.host_ids_str")

    val (vertexRDD, edgeRDD) = (graph.vertices, graph.edges)
    //    log.info(s"vertices count ${vertexRDD.count()}, edge cout ${edgeRDD.count()}")
    log.info(s"num vertices ${vertexRDD.count()}, num edges ${edgeRDD.count()}")

    gsSession.close()
    sc.stop()
  }
}
