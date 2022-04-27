package org.apache.spark.graphx

import com.alibaba.graphscope.graphx.JavaVertexPartition
import org.apache.spark.graphx.impl.offheap.OffHeapVertexRDDImpl
import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, SparkContext}

import scala.reflect.ClassTag

/**
 * Act as the base class of gs related rdds.
 */
abstract class GrapeVertexRDD[VD](
                                   sc: SparkContext, deps: Seq[Dependency[_]]) extends VertexRDD[VD](sc, deps) {

  private[graphx] def mapGrapeVertexPartitions[VD2: ClassTag](
                                                   f: GrapeVertexPartition[VD] => GrapeVertexPartition[VD2])
  : GrapeVertexRDD[VD2];

  private[graphx] def withGrapePartitionsRDD[VD2 : ClassTag](partitionsRDD: RDD[(PartitionID, GrapeVertexPartition[VD2])])
  : GrapeVertexRDD[VD2]
}

object GrapeVertexRDD {
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

  def fromVertexPartitions[VD : ClassTag](vertexPartition : RDD[(PartitionID, GrapeVertexPartition[VD])]): OffHeapVertexRDDImpl[VD] ={
    new OffHeapVertexRDDImpl[VD](vertexPartition)
  }

}
