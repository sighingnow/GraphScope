package org.apache.spark.graphx


import org.apache.spark.graphx.impl.grape.GrapeVertexRDDImpl
import org.apache.spark.graphx.impl.partition.{GrapeVertexPartition, GrapeVertexPartitionBuilder}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Dependency, SparkContext}

import scala.reflect.ClassTag

/**
 * Act as the base class of gs related rdds.
 */
abstract class GrapeVertexRDD[VD](
                                   sc: SparkContext, deps: Seq[Dependency[_]]) extends VertexRDD[VD](sc, deps) {
  private[graphx] def grapePartitionsRDD: RDD[(PartitionID, GrapeVertexPartition[VD])]

  override def partitionsRDD = null

  private[graphx] def mapGrapeVertexPartitions[VD2: ClassTag](
                                                   f: GrapeVertexPartition[VD] => GrapeVertexPartition[VD2])
  : GrapeVertexRDD[VD2];

  private[graphx] def withGrapePartitionsRDD[VD2 : ClassTag](partitionsRDD: RDD[(PartitionID, GrapeVertexPartition[VD2])])
  : GrapeVertexRDD[VD2]

  def mapVertices[VD2: ClassTag](map: (VertexId, VD) => VD2) : GrapeVertexRDD[VD2]
  /**
   * Write the updated vertex data to memory mapped region.
   */
//  def writeBackVertexData(vdataMappedPath : String, size : Long): Unit

  /**
   * Create a new vertex rdd which contains the data updated from shared memeory
   */
  def withGrapeVertexData(vdataMappedPath: String, size : Long) : GrapeVertexRDD[VD]
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

  def fromVertexPartitions[VD : ClassTag](vertexPartition : RDD[(PartitionID, GrapeVertexPartition[VD])]): GrapeVertexRDDImpl[VD] ={
    new GrapeVertexRDDImpl[VD](vertexPartition)
  }

  def fromEdgeRDD[VD: ClassTag](edgeRDD: GrapeEdgeRDD[_], numPartitions : Int, defaultVal : VD, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY) : GrapeVertexRDDImpl[VD] = {
    log.info(s"Driver: Creating vertex rdd from edgeRDD of numPartition ${numPartitions}, default val ${defaultVal}")
    val grapePartition = edgeRDD.grapePartitionsRDD.mapPartitions(
      iter => {
        val tuple = iter.next()
        val epart = tuple._2
        val vertexPartitionBuilder = new GrapeVertexPartitionBuilder[VD]
        val fragVertices = epart.vm.getVertexSize
        log.info(s"Partition ${tuple._1} doing initialization with default value ${defaultVal}, frag vertices ${fragVertices}")
        vertexPartitionBuilder.init(fragVertices, defaultVal)
        val grapeVertexPartition = vertexPartitionBuilder.build(tuple._1, epart.client, epart.vm)
        Iterator((tuple._1, grapeVertexPartition))
      }
    ).cache()
    new GrapeVertexRDDImpl[VD](grapePartition,storageLevel)
  }
}
