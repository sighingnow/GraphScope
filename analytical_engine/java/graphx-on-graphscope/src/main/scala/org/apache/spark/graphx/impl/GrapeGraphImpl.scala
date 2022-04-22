package org.apache.spark.graphx.impl

import org.apache.spark.graphx.{GrapeEdgeRDD, GrapeVertexRDD, PartitionID}
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

class GrapeGraphImpl[VD :ClassTag, ED: ClassTag](vertices: GrapeVertexRDD[VD],
                                                 edges: GrapeEdgeRDD[ED]) {

  def numVertices : Long = vertices.count()
}


object GrapeGraphImpl{

  def fromExistingRDDs[VD: ClassTag, ED: ClassTag](
                              vertices: GrapeVertexRDD[VD],
                              edges: GrapeEdgeRDD[ED]): GrapeGraphImpl[VD, ED] = {
    new GrapeGraphImpl[VD,ED](vertices, edges)
  }
  def fromEdgeRDD[VD: ClassTag, ED: ClassTag](
      edgeRdd: GrapeEdgeRDDImpl[ED],
      defaultVertexAttr: VD,
      edgeStorageLevel : StorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel: StorageLevel= StorageLevel.MEMORY_ONLY): GrapeGraphImpl[VD, ED] = {
    val edgesCached = edgeRdd.withTargetStorageLevel(edgeStorageLevel).cache()
    val vertices =
      GrapeVertexRDD.fromEdges(edgesCached, edgesCached.partitions.length, defaultVertexAttr)
        .withTargetStorageLevel(vertexStorageLevel)
    fromExistingRDDs(vertices, edgesCached)
  }
}