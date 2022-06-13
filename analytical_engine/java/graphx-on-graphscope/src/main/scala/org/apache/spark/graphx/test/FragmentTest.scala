package org.apache.spark.graphx.test

import com.alibaba.graphscope.graphx.{GSSession, GraphScopeHelper}
import org.apache.spark.graphx.impl.GrapeGraphImpl
import org.apache.spark.graphx.rdd.GraphScopeRDD
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

//    val graph: GrapeGraphImpl[Long, Long] = gsSession.loadGraph("import graphscope\n" +
//      "sess = graphscope.session(cluster_type=\"hosts\",hosts=[\"d50\"], num_workers=1,etcd_addrs=\"http://11.227.236.89:2379\",vineyard_socket=\"/tmp/vineyard.sock\")\n" +
//      "graph = sess.g(directed=True)\n" +
//      "graph = graph.add_vertices(\"/home/graphscope/data/livejournal.v\",\"person\")\n" +
//      "graph = graph.add_edges(\"/home/graphscope/data/livejournal.e\",label=\"knows\",src_label=\"person\",dst_label=\"person\")\n" +
//      "graph_proj = graph.project(vertices={\"person\":[\"weight\"]}, edges={\"knows\" : [\"dist\"]})\n" +
//      "simple = graph_proj._project_to_simple()\n", "simple")
    val graph : GrapeGraphImpl[Long,Long] = GraphScopeRDD.loadFragmentAsGraph[Long,Long](sc, "d50:250271883936897154", "gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>")

    val (vertexRDD, edgeRDD) = (graph.vertices, graph.edges)
    log.info(s"num vertices ${vertexRDD.count()}, num edges ${edgeRDD.count()}")

    val time0 = System.nanoTime()
    val graph2 = graph.mapTriplets(triplet => {
      triplet.srcAttr + triplet.dstAttr + triplet.attr
    })
    val graph3 = graph2.mapTriplets(triplet => {
      triplet.attr + 1
    })
    val graph4 = graph3.mapTriplets(triplet => {
      triplet.attr + 1
    })
    log.info(s"mapping triplet,edges num ${graph4.numEdges}, vertices num ${graph4.numVertices}")
    val time1 = System.nanoTime()

    val time2 = System.nanoTime()
    val graph5 = graph.mapVertices((vid, vd)=> vid + vd)
    val graph6 = graph5.mapVertices((vid, vd)=> vd.toLong)
    val graph7 = graph6.mapVertices((vid, vd)=> vd + vid)
    log.info(s"map vertices ,edges num ${graph7.numEdges}, vertices num ${graph7.numVertices}")
    val time3 = System.nanoTime()


    val time4 = System.nanoTime()
    val outDegree = graph.outDegrees.cache()
    val inDegree = graph.inDegrees.cache()
    log.info(s"after get out degree and in degrees ${outDegree.count()}, ${inDegree.count()}")
    val time5 = System.nanoTime()
    val graph16 = graph.outerJoinVertices(outDegree)((_,_,degree) => degree.getOrElse(0))
    val graph17 = graph.outerJoinVertices(inDegree)((_,_,degree)=>degree.getOrElse(0))
    log.info(s"outer joined vertices: vertices ${graph16.numVertices}, vertices ${graph17.numVertices}")
    val time6 = System.nanoTime()
    log.info(s"mapping triplet cost ${(time1 - time0)/1000000}ms")
    log.info(s"map vertices cost ${(time3 - time2)/1000000}ms")
    log.info(s"getting degree cost ${(time5 - time4)/1000000} ms")
    log.info(s"outer join cost ${(time6 - time5)/1000000} ms")
    gsSession.close()
    sc.stop()
//    gsSession.runOnGAE("from graphscope import sssp\n" +
//      "sssp({}, src=1)", graph)
//    log.info("testing running sssp on modified graph")
//    gsSession.runOnGAE("from graphscope import sssp\n" +
//      "sssp({}, src=1)", graph2)
  }
}
