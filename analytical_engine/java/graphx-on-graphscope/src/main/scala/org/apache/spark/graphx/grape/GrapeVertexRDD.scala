package org.apache.spark.graphx.grape

import com.alibaba.graphscope.ds.Vertex
import com.alibaba.graphscope.fragment.IFragment
import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.graphx.graph.GraphStructure
import com.alibaba.graphscope.graphx.graph.impl.FragmentStructure
import com.alibaba.graphscope.graphx.rdd.RoutingTable
import com.alibaba.graphscope.graphx.rdd.impl.GrapeVertexPartition
import com.alibaba.graphscope.graphx.shuffle.EdgeShuffle
import com.alibaba.graphscope.graphx.store.InHeapVertexDataStore
import com.alibaba.graphscope.graphx.utils.GrapeUtils
import com.alibaba.graphscope.utils.FFITypeFactoryhelper
import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.spark.graphx._
import org.apache.spark.graphx.grape.impl.GrapeVertexRDDImpl
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

  def syncOuterVertex : GrapeVertexRDD[VD]

  def collectNbrIds(direction : EdgeDirection) : GrapeVertexRDD[Array[VertexId]]

  /** TODO: create a new vertex rdd with new vd array */
}

object GrapeVertexRDD extends Logging{

  def fromVertexPartitions[VD : ClassTag](vertexPartition : RDD[GrapeVertexPartition[VD]]): GrapeVertexRDDImpl[VD] ={
    new GrapeVertexRDDImpl[VD](vertexPartition)
  }

  def fromEdgeShuffle[VD : ClassTag, ED : ClassTag](edgeShuffle : RDD[(PartitionID, EdgeShuffle[VD,ED])], edgeRDD : GrapeEdgeRDD[ED],storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY) : GrapeVertexRDDImpl[VD] = {
    val numPartitions = edgeRDD.getNumPartitions
    log.info(s"Creating vertex rdd from grape edgeRDD of numPartition ${numPartitions}, along with vertex attrs ")
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
    val ePartWithRoutingTables = edgeRDD.grapePartitionsRDD.zipPartitions(routingTableMessage){
      (edgeIter,msgIter) => {
        val ePart = edgeIter.next()
        val routingTable = RoutingTable.fromMsg(ePart.pid, numPartitions,msgIter, ePart.graphStructure)
        Iterator((ePart, routingTable))
      }
    }.cache()
    log.info(s"epart with routing table count ${ePartWithRoutingTables.count()}")

    val grapeVertexPartition = ePartWithRoutingTables.zipPartitions(edgeShuffle){
      (firstIter, edgeShuffleIter) => {
        val (ePart, routingTable) = firstIter.next()
        val fragVertices = ePart.graphStructure.getVertexSize
//        val (edgeShufflePid, edgeShuffle) = edgeShuffleIter.next()
//        require(edgeShufflePid == ePart.pid)
        log.info(s"Partition ${ePart.pid} doing initialization with graphx vertex attrs, frag vertices ${fragVertices}")
        val grapeVertexPartition = buildPartitionFromGraphX(ePart.pid, ePart.client, ePart.graphStructure, routingTable, edgeShuffleIter)
        Iterator(grapeVertexPartition)
      }
    }.cache()
    new GrapeVertexRDDImpl[VD](grapeVertexPartition, storageLevel)
  }

  def buildPartitionFromGraphX[VD: ClassTag](pid: Int, client: VineyardClient, graphStructure: GraphStructure, routingTable: RoutingTable, edgeShuffleIter: Iterator[(PartitionID,EdgeShuffle[VD,_])]):GrapeVertexPartition[VD] = {
    /** We assume the verticesAttr iterator contains only inner vertices */
    val newArray = PrimitiveArray.create(GrapeUtils.getRuntimeClass[VD], graphStructure.getVertexSize.toInt).asInstanceOf[PrimitiveArray[VD]]
    val grapeVertex = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
    while (edgeShuffleIter.hasNext){
      val (dstPid, edgeShuffle) = edgeShuffleIter.next()
      require(dstPid == pid)
      val oids = edgeShuffle.oids
      val verticesAttr = edgeShuffle.vertexAttrs
      require(oids.length == verticesAttr.length, s"neq ${oids.length}, ${verticesAttr.length}")
      val len = oids.length
      var i = 0
      while (i < len){
        val oid = oids(i)
        val vdata = verticesAttr(i)
        require(graphStructure.getInnerVertex(oid,grapeVertex))
        newArray.set(grapeVertex.GetValue(), vdata)
        i += 1
      }
    }
    val newVertexData = new InHeapVertexDataStore[VD](newArray,client)
    new GrapeVertexPartition[VD](pid, graphStructure, newVertexData, client, routingTable)
  }

  def fromGrapeEdgeRDD[VD: ClassTag](edgeRDD: GrapeEdgeRDD[_], numPartitions : Int, defaultVal : VD, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY) : GrapeVertexRDDImpl[VD] = {
    log.info(s"Driver: Creating vertex rdd from graphx edgeRDD of numPartition ${numPartitions}, default val ${defaultVal}")
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
        val fragVertices = ePart.graphStructure.getVertexSize
        log.info(s"Partition ${ePart.pid} doing initialization with default value ${defaultVal}, frag vertices ${fragVertices}")
        val grapeVertexPartition = GrapeVertexPartition.buildPrimitiveVertexPartition(fragVertices,defaultVal,ePart.pid, ePart.client, ePart.graphStructure, routingTable)
        Iterator(grapeVertexPartition)
      }
    }.cache()
    new GrapeVertexRDDImpl[VD](grapeVertexPartition,storageLevel)
  }

  def fromFragmentEdgeRDD[VD: ClassTag](edgeRDD: GrapeEdgeRDD[_], numPartitions : Int, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY) : GrapeVertexRDDImpl[VD] = {
    log.info(s"Driver: Creating vertex rdd from fragment edgeRDD of numPartition ${numPartitions}")
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
    val grapeVertexPartitions = edgeRDD.grapePartitionsRDD.zipPartitions(routingTableMessage){
      (edgeIter,msgIter) => {
        val ePart = edgeIter.next()
        val routingTable = RoutingTable.fromMsg(ePart.pid, numPartitions,msgIter, ePart.graphStructure)
        val array = PrimitiveArray.create(GrapeUtils.getRuntimeClass[VD], ePart.graphStructure.getVertexSize.toInt).asInstanceOf[PrimitiveArray[VD]]
        val actualStructure = ePart.graphStructure.asInstanceOf[FragmentStructure]
        val frag = actualStructure.fragment.asInstanceOf[IFragment[Long,Long,VD,_]]
        val vertex = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[Long]]
        for (i <- 0 until ePart.graphStructure.getInnerVertexSize.toInt){
          vertex.SetValue(i)
          array.set(i,frag.getData(vertex))
        }
        //only set inner vertices
        val vertexDataStore = new InHeapVertexDataStore[VD](array, ePart.client)
        val partition = new GrapeVertexPartition[VD](ePart.pid, actualStructure, vertexDataStore, ePart.client, routingTable)
        Iterator(partition)
      }
    }.cache()
    new GrapeVertexRDDImpl[VD](grapeVertexPartitions,storageLevel)
  }
}
