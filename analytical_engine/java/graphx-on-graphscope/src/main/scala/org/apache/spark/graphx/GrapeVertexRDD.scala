package org.apache.spark.graphx


import com.alibaba.graphscope.graphx.graph.impl.GraphXGraphStructure
import org.apache.spark.graphx.impl.grape.GrapeVertexRDDImpl
import org.apache.spark.graphx.impl.partition.{GrapeVertexPartition, GrapeVertexPartitionBuilder, RoutingTable}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Dependency, HashPartitioner, SparkContext}

import scala.reflect.ClassTag

/**
 * Act as the base class of gs related rdds.
 */
abstract class GrapeVertexRDD[VD](
                                   sc: SparkContext, deps: Seq[Dependency[_]]) extends VertexRDD[VD](sc, deps) {
  private[graphx] def grapePartitionsRDD: RDD[GrapeVertexPartition[VD]]

  override def partitionsRDD = null

  private[graphx] def mapGrapeVertexPartitions[VD2: ClassTag](
                                                               f: GrapeVertexPartition[VD] => GrapeVertexPartition[VD2])
  : GrapeVertexRDD[VD2];

  private[graphx] def withGrapePartitionsRDD[VD2: ClassTag](partitionsRDD: RDD[GrapeVertexPartition[VD2]])
  : GrapeVertexRDD[VD2]

  def mapVertices[VD2: ClassTag](map: (VertexId, VD) => VD2): GrapeVertexRDD[VD2]

  override def innerZipJoin[U: ClassTag, VD2: ClassTag](other: VertexRDD[U])
                                              (f: (VertexId, VD, U) => VD2): GrapeVertexRDD[VD2]

  override def innerJoin[U : ClassTag, VD2 : ClassTag](other: RDD[(VertexId, U)])(f: (VertexId, VD, U) => VD2): GrapeVertexRDD[VD2]

  override def leftJoin[VD2: ClassTag, VD3: ClassTag](other: RDD[(VertexId, VD2)])(f: (VertexId, VD, Option[VD2]) => VD3)
  : GrapeVertexRDD[VD3]
  override def leftZipJoin[VD2: ClassTag, VD3: ClassTag]
  (other: VertexRDD[VD2])(f: (VertexId, VD, Option[VD2]) => VD3): VertexRDD[VD3]
}

object GrapeVertexRDD extends Logging{
  /**
   * Constructs a `VertexRDD` containing all vertices referred to in `edges`. The vertices will be
   * created with the attribute `defaultVal`. The resulting `VertexRDD` will be joinable with
   * `edges`.
   *
   * @tparam VD the vertex attribute type
   * @param edges         the [[EdgeRDD]] referring to the vertices to create
   * @param numPartitions the desired number of partitions for the resulting `VertexRDD`
   * @param defaultVal    the vertex attribute to use when creating missing vertices
   */
  def fromEdges[VD: ClassTag](
                               edges: EdgeRDD[_], numPartitions: Int, defaultVal: VD): GrapeVertexRDD[VD] = {

    null
  }

  def fromVertexPartitions[VD : ClassTag](vertexPartition : RDD[GrapeVertexPartition[VD]]): GrapeVertexRDDImpl[VD] ={
    new GrapeVertexRDDImpl[VD](vertexPartition)
  }

  def fromEdgeRDD[VD: ClassTag](edgeRDD: GrapeEdgeRDD[_], numPartitions : Int, defaultVal : VD, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY) : GrapeVertexRDDImpl[VD] = {
    log.info(s"Driver: Creating vertex rdd from edgeRDD of numPartition ${numPartitions}, default val ${defaultVal}")
    //creating routing table
    val routingTableMessage = edgeRDD.grapePartitionsRDD.mapPartitions(iter => {
      if (iter.hasNext){
        val part = iter.next()
        RoutingTable.generateMsg(part.pid, numPartitions, part.graphStructure)
      }
      else {
        Iterator.empty
      }
    }).partitionBy(new HashPartitioner(numPartitions)).cache()
    val grapeVertexPartition = edgeRDD.grapePartitionsRDD.zipPartitions(routingTableMessage){
      (edgeIter,msgIter) => {
        val ePart = edgeIter.next()
        val routingTable = RoutingTable.fromMsg(ePart.pid, numPartitions,msgIter, ePart.graphStructure)
        val vertexPartitionBuilder = new GrapeVertexPartitionBuilder[VD]
        val fragVertices = ePart.graphStructure.getVertexSize
        log.info(s"Partition ${ePart.pid} doing initialization with default value ${defaultVal}, frag vertices ${fragVertices}")
        vertexPartitionBuilder.init(fragVertices, defaultVal)
        val grapeVertexPartition = vertexPartitionBuilder.build(ePart.pid, ePart.client, ePart.graphStructure, routingTable)
        Iterator(grapeVertexPartition)
      }
    }.cache()
    new GrapeVertexRDDImpl[VD](grapeVertexPartition,storageLevel)
  }
}
