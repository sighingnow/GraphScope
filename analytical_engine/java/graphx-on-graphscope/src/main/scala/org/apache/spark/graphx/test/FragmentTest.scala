package org.apache.spark.graphx.test

import com.alibaba.graphscope.graphx.{GSSession, GraphScopeHelper}
import org.apache.spark.graphx.impl.GrapeGraphImpl
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object FragmentTest extends Logging{
  def main(array: Array[String]) : Unit = {
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    val gsSession: GSSession = GraphScopeHelper.createSession(sc)

    val graph: GrapeGraphImpl[Long, Long] = gsSession.loadGraph("import graphscope\n" +
      "sess = graphscope.session(cluster_type=\"hosts\",hosts=[\"d50\"], num_workers=1,etcd_addrs=\"http://11.227.236.89:2379\",vineyard_socket=\"/tmp/vineyard.sock\")\n" +
      "graph = sess.g(directed=True)\n" +
      "graph = graph.add_vertices(\"/home/graphscope/data/gstest/property/p2p-31_property_v_0\",\"person\")\n" +
      "graph = graph.add_edges(\"/home/graphscope/data/gstest/property/p2p-31_property_e_0\",label=\"knows\",src_label=\"person\",dst_label=\"person\")\n" +
      "graph_proj = graph.project(vertices={\"person\":[\"weight\"]}, edges={\"knows\" : [\"dist\"]})\n" +
      "simple = graph_proj._project_to_simple()\n", "simple")

    val (vertexRDD, edgeRDD) = (graph.vertices, graph.edges)
    //    log.info(s"vertices count ${vertexRDD.count()}, edge cout ${edgeRDD.count()}")
    log.info(s"num vertices ${vertexRDD.count()}, num edges ${edgeRDD.count()}")
    val graph2 = graph.mapTriplets(triplet => {
//      log.info(s"visiting edge triplet [src: (${triplet.srcId}, ${triplet.srcAttr}), dst: (${triplet.dstId}, ${triplet.dstAttr}), attr: ${triplet.attr}]")
      triplet.srcAttr + triplet.dstAttr + triplet.attr
    })

    log.info(s"mapping triplet,edges num ${graph2.numEdges}, vertices num ${graph2.numVertices}")

    gsSession.close()
    sc.stop()
  }
}
