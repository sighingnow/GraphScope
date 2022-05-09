package org.apache.spark.graphx

import org.apache.spark.graphx.impl.GrapeVertexPartitionWrapper
import org.apache.spark.graphx.impl.grape.GrapeVertexRDDImpl
import org.apache.spark.graphx.utils.GrapeVertexPartitionRegistry
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, HashPartitioner, SparkContext}

import scala.reflect.ClassTag

/**
 * Act as the base class of gs related rdds.
 */
abstract class GrapeVertexRDD[VD](
                                   sc: SparkContext, deps: Seq[Dependency[_]]) extends VertexRDD[VD](sc, deps) {

  private[graphx] def mapGrapeVertexPartitions[VD2: ClassTag](
                                                   f: GrapeVertexPartitionWrapper[VD] => GrapeVertexPartitionWrapper[VD2])
  : GrapeVertexRDD[VD2];

  private[graphx] def withGrapePartitionsRDD[VD2 : ClassTag](partitionsRDD: RDD[(PartitionID, GrapeVertexPartitionWrapper[VD2])])
  : GrapeVertexRDD[VD2]

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

  def fromVertexPartitions[VD : ClassTag](vertexPartition : RDD[(PartitionID, GrapeVertexPartitionWrapper[VD])]): GrapeVertexRDDImpl[VD] ={
    new GrapeVertexRDDImpl[VD](vertexPartition)
  }

  def fromEdgeRDD[VD: ClassTag](edgeRDD: GrapeEdgeRDD[_], numPartitions : Int, defaultVal : VD) : GrapeVertexRDD[VD] = {
    log.info(s"Driver: Creating vertex rdd from edgeRDD of numPartition ${numPartitions}, default val ${defaultVal}")
    //First creating partial vertex map. We may not need to use it in graphx. just pass it to c++ to build.
    val vertexPartitions = createVertexPartitions(numPartitions, edgeRDD, defaultVal)
    //Pass to c++ for building
//    val vertexPartition = GrapeVertexRDD.fromPartitionBuilder(vertexPartitionBuilderRDD, defaultVal)
    fromVertexPartitions(vertexPartitions)
  }

  def createVertexPartitions[VD: ClassTag](numPartitions : Int, edgeRDD: GrapeEdgeRDD[_], vd: VD): RDD[(PartitionID, GrapeVertexPartitionWrapper[VD])] ={
    val partitioner = new HashPartitioner(numPartitions)
    val vertexShuffles = edgeRDD.grapePartitionsRDD.mapPartitions(iter => {
      val tuple = iter.next();
      tuple._2.generateVertexShuffles(partitioner)
    }).partitionBy(partitioner)

    val tmp = vertexShuffles.mapPartitionsWithIndex((ind,iter) => {
      val registry = GrapeVertexPartitionRegistry.getOrCreate[VD]
      registry.createVertexPartitionBuilder(ind)
      val vertexPartitionBuilder = registry.getVertexPartitionBuilder()
      //VertexShuffle to std::vector.
      var cnt = 0
      while (iter.hasNext){
        val (pid,shuffle) = iter.next()
        require(pid == ind)
        require(pid == shuffle.dstPid)
        val vec = shuffle.toVector
        log.info(s"Partition ${ind} adding shuffles from ${shuffle.fromPid}, size ${shuffle.size()}")
        vertexPartitionBuilder.addVertex(vec, shuffle.fromPid)
	cnt += shuffle.size()
      }
      Iterator((ind,cnt))
    })
    log.info(s"total: shuffles ${tmp.cache().count()}")

    //Builder
    tmp.foreachPartition(iter => {
      if (iter.hasNext){
           val registry = GrapeVertexPartitionRegistry.getOrCreate[VD]
          registry.build(iter.next()._1, vd)
      }
    })

    val vertexPartitionsRDD = tmp.mapPartitions(iter => {
      if (iter.hasNext){
          val firstOne = iter.next()
          val registry = GrapeVertexPartitionRegistry.getOrCreate[VD]
          Iterator((firstOne._1, registry.getGrapeVertexPartitionWrapper(firstOne._1,numPartitions)))
      }
      else {
	      Iterator.empty
      }
    }).cache()
    log.info(s"Finish constructing partition wrappers, num: ${vertexPartitionsRDD.count()}")

    vertexPartitionsRDD
  }
}
