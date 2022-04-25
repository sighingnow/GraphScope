package org.apache.spark.graphx

import org.apache.spark.graphx.impl.{GrapeEdgePartition, GrapeEdgeRDDImpl, GrapeVertexPartition, GrapeVertexRDDImpl, ShippableVertexPartition}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

abstract class GrapeVertexRDD[VD](
     sc: SparkContext, deps: Seq[Dependency[_]]) extends VertexRDD[VD](sc, deps){
  def mapToFile(filePrefix : String, mappedSize : Long) : Array[String]

  //We should not call RDD.getNumPartitions.
  def numPartitions : Int


  override def compute(part: Partition, context: TaskContext): Iterator[(VertexId,VD)] = {
    val p = firstParent[(Int, GrapeVertexPartition[VD])].iterator(part, context)
    if (p.hasNext) {
      p.next()._2.iterator.map(_.copy())
    }
    else {
      Iterator.empty
    }
  }

  def createMapFilePerExecutor(filepath: String, mappedSize : Long): Unit;

  def copyAndUpdateVertexData(filePath : String , mappedSize :Long) : GrapeVertexRDD[VD]

  private[graphx] def mapGrapeVertexPartitions[VD2: ClassTag](
          f: GrapeVertexPartition[VD] => GrapeVertexPartition[VD2])
  : GrapeVertexRDD[VD2]

  private[graphx] def withGrapePartitionsRDD[VD2: ClassTag](
      partitionsRDD: RDD[(PartitionID, GrapeVertexPartition[VD2])]): GrapeVertexRDD[VD2]
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
