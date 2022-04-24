package org.apache.spark.graphx

import org.apache.spark.graphx.impl.{GrapeEdgeRDDImpl, GrapeVertexPartition, GrapeVertexRDDImpl}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, SparkContext}

import scala.reflect.ClassTag

abstract class GrapeVertexRDD[VD](
     sc: SparkContext, deps: Seq[Dependency[_]]) extends VertexRDD[VD](sc, deps){
  def mapToFile(filePrefix : String, mappedSize : Long) : Array[String]
}

object GrapeVertexRDD{
  /**
   * Constructs a `VertexRDD` containing all vertices referred to in `edges`. The vertices will be
   * created with the attribute `defaultVal`. The resulting `VertexRDD` will be joinable with
   * `edges`.
   *
   * @tparam VD the vertex attribute type
   *
   * @param edges the [[EdgeRDD]] referring to the vertices to create
   * @param numPartitions the desired number of partitions for the resulting `VertexRDD`
   * @param defaultVal the vertex attribute to use when creating missing vertices
   */
  def fromEdges[VD: ClassTag](
           edges: EdgeRDD[_], numPartitions: Int, defaultVal: VD): GrapeVertexRDDImpl[VD] = {

    val vertexMapRDD = edges.asInstanceOf[GrapeEdgeRDDImpl[_]].createVertexMapRDD()
    val vertexPartitions : RDD[(PartitionID, GrapeVertexPartition[VD])]= vertexMapRDD.mapPartitions({
      iter => {
        if (iter.hasNext){
          val tuple = iter.next()
          Iterator((tuple._1, tuple._2.toVertexPartition(defaultVal)))
        }
        else {
          Iterator.empty
        }
      }
    })
    new GrapeVertexRDDImpl[VD](vertexPartitions)
  }
//    val routingTables = createRoutingTables(edges, new HashPartitioner(numPartitions))
//    val vertexPartitions = routingTables.mapPartitions({ routingTableIter =>
//      val routingTable =
//        if (routingTableIter.hasNext) routingTableIter.next() else RoutingTablePartition.empty
//      Iterator(ShippableVertexPartition(Iterator.empty, routingTable, defaultVal))
//    }, preservesPartitioning = true)
//    new VertexRDDImpl(vertexPartitions)

}
