package org.apache.spark.graphx.grape

import com.alibaba.graphscope.graphx.graph.GraphStructureTypes.GraphStructureType
import com.alibaba.graphscope.graphx.graph.impl.GraphXGraphStructure
import com.alibaba.graphscope.graphx.shuffle.EdgeShuffle
import com.alibaba.graphscope.graphx.store.InHeapVertexDataStore
import com.alibaba.graphscope.graphx.utils.{BitSetWithOffset, ExecutorUtils, GrapeUtils, ScalaFFIFactory}
import org.apache.spark.graphx._
import org.apache.spark.graphx.grape.impl.{GrapeEdgeRDDImpl, GrapeVertexRDDImpl}
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.{BitSet, OpenHashSet, PrimitiveVector}
import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.{ClassTag, classTag}

object GrapeGraphBackend extends Enumeration{
  val GraphXFragment, ArrowProjectedFragment = GrapeGraphBackend
}
/**
 * Creating a graph abstraction by combining vertex RDD and edge RDD together.
 * Before doing this construction:
 *   - Both vertex RDD and edge RDD are available for map,fliter operators.
 *   - Both vertex RDD and edge RDD stores data in partitions
 *     When construct this graph, we will
 *   - copy data out to shared-memory
 *   - create mpi processes to load into fragment.
 *   - Wrap fragment as GrapeGraph when doing pregel computation.
 *     When changes made to graphx graph, it will not directly take effect on grape-graph. To apply these
 *     changes to grape-graph. Invoke to grape graph directly.
 *
 * The vertexRDD and EdgeRDD are designed in a way that, we can construct the whole graph without
 * any shuffle in MPI.
 *
 * @param vertices vertex rdd
 * @param edges    edge rdd
 * @tparam VD vd
 * @tparam ED ed
 */
class GrapeGraphImpl[VD: ClassTag, ED: ClassTag] protected(
                                                            @transient val vertices: GrapeVertexRDD[VD],
                                                            @transient val edges: GrapeEdgeRDD[ED]) extends Graph[VD, ED] with Serializable {
  val logger: Logger = LoggerFactory.getLogger(classOf[GrapeGraphImpl[_,_]].toString)
  vertices.cache()
  edges.cache()


  lazy val backend: GraphStructureType = vertices.grapePartitionsRDD.mapPartitions(iter => {
    val part = iter.next()
    Iterator(part.graphStructure.structureType)
  }).collect().distinct(0)
//  logger.info(s"Creating grape graph backended by ${backend}")

  val vdClass: Class[VD] = classTag[VD].runtimeClass.asInstanceOf[java.lang.Class[VD]]
  val edClass: Class[ED] = classTag[ED].runtimeClass.asInstanceOf[java.lang.Class[ED]]
  val grapeEdges: GrapeEdgeRDDImpl[VD, ED] = edges.asInstanceOf[GrapeEdgeRDDImpl[VD,ED]]
  val grapeVertices: GrapeVertexRDDImpl[VD] = vertices.asInstanceOf[GrapeVertexRDDImpl[VD]]

  def numVertices: Long = grapeVertices.count()

  def numEdges: Long = edges.count()


  lazy val fragmentIds : RDD[String] = {
    val syncedGrapeVertices = grapeVertices.syncOuterVertex

    PartitionAwareZippedBaseRDD.zipPartitions(SparkContext.getOrCreate(),
      edges.grapePartitionsRDD,
      syncedGrapeVertices.grapePartitionsRDD){(edgeIter, vertexIter) => {
      val ePart = edgeIter.next()
      val vPart = vertexIter.next()
      ePart.graphStructure match {
        case casted: GraphXGraphStructure =>
          val vmId = casted.vm.id() //vm id will never change
          val csrId = casted.csr.id()
          val vdId = vPart.innerVertexData.vineyardID
          //FIXME: merge edata array together.
          val edId = GrapeUtils.array2ArrowArray[ED](ePart.edatas.array,ePart.client,false)

          logger.info(s"vm id ${vmId}, csr id ${csrId}, vd id ${vdId}, ed id ${edId}")

          var fragId = null.asInstanceOf[Long]
          if (GrapeUtils.isPrimitive[VD] && GrapeUtils.isPrimitive[ED]){
            logger.info("VD and ED both primitive")
            val fragBuilder = ScalaFFIFactory.newGraphXFragmentBuilder[VD, ED](ePart.client, vmId, csrId, vdId,edId)
            fragId = fragBuilder.seal(ePart.client).get().id()
          }
          else if (GrapeUtils.isPrimitive[VD]){
            logger.info("only vd primitive")
            val fragBuilder = ScalaFFIFactory.newGraphXStringEDFragmentBuilder[VD](ePart.client, vmId, csrId, vdId, edId)
            fragId = fragBuilder.seal(ePart.client).get().id()
          }
          else if (GrapeUtils.isPrimitive[ED]){
            logger.info("only ed primitive")
            val fragBuilder = ScalaFFIFactory.newGraphXStringVDFragmentBuiler[ED](ePart.client, vmId, csrId, vdId,edId)
            fragId = fragBuilder.seal(ePart.client).get().id()
          }
          else {
            logger.info("vd and ed are both complex")
            val fragBuilder = ScalaFFIFactory.newGraphXStringVEDFragmentBuilder(ePart.client, vmId, csrId, vdId, edId)
            fragId = fragBuilder.seal(ePart.client).get().id()
          }
          logger.info(s"Got built frag: ${fragId}")
          Iterator(ExecutorUtils.getHostName + ":" + ePart.pid + ":" + fragId)
        case _ =>
          throw new IllegalStateException("Not implemented now!")
      }
    }}
  }

  //FIXME: refactor this into construct graphxFragment from here
  def generateGlobalVMIds() : Array[String] = {
    edges.grapePartitionsRDD.mapPartitions(iter => {
      if (iter.hasNext){
        val part = iter.next()
        part.graphStructure match {
          case casted: GraphXGraphStructure =>
            Iterator(ExecutorUtils.getHostName + ":" + part.pid + ":" + casted.vm.id())
          case _ =>
            throw new IllegalStateException("Not implemented now!")
        }
      }
      else {
        Iterator.empty
      }
    }).collect()
  }


  @transient override lazy val triplets: RDD[EdgeTriplet[VD, ED]] = {
    PartitionAwareZippedBaseRDD.zipPartitions(SparkContext.getOrCreate(), grapeEdges.grapePartitionsRDD, grapeVertices.grapePartitionsRDD){
      (edgeIter, vertexIter) => {
        val edgePart = edgeIter.next()
        val vertexPart = vertexIter.next()
        edgePart.tripletIterator(vertexPart.innerVertexData,vertexPart.outerVertexData)
      }
    }
  }

  override def persist(newLevel: StorageLevel): Graph[VD, ED] = {
    vertices.persist(newLevel)
    edges.persist(newLevel)
    this
  }

  override def cache(): Graph[VD, ED] = {
    vertices.cache()
    edges.cache()
    this
  }

  override def checkpoint(): Unit = {
    vertices.checkpoint()
    edges.checkpoint()
  }

  override def isCheckpointed: Boolean = {
    vertices.isCheckpointed && edges.isCheckpointed
  }

  override def getCheckpointFiles: Seq[String] = {
    Seq(vertices.getCheckpointFile, edges.getCheckpointFile).flatMap {
      case Some(path) => Seq(path)
      case None => Seq.empty
    }
  }

  override def unpersist(blocking: Boolean): Graph[VD, ED] = {
    vertices.unpersist(blocking)
    edges.unpersist(blocking)
    this
  }

  override def unpersistVertices(blocking: Boolean): Graph[VD, ED] = {
    vertices.unpersist(blocking)
    this
  }

  override def partitionBy(partitionStrategy: PartitionStrategy): Graph[VD, ED] = {
    logger.warn("Currently grape graph doesn't support partition")
    this
  }

  override def partitionBy(partitionStrategy: PartitionStrategy, numPartitions: PartitionID): Graph[VD, ED] = {
    logger.warn("Currently grape graph doesn't support partition")
    this
  }
  def mapVertices[VD2: ClassTag](map: (VertexId, VD) => VD2)
                                (implicit eq: VD =:= VD2 = null): Graph[VD2, ED] = {
//    vertices.cache()
    new GrapeGraphImpl[VD2,ED](vertices.mapVertices[VD2](map), edges)
  }
  override def mapEdges[ED2: ClassTag](map: Edge[ED] => ED2): Graph[VD, ED2] = {
    val newEdgePartitions = grapeEdges.grapePartitionsRDD.mapPartitions(
      iter => {
        if (iter.hasNext){
          val part = iter.next()
          Iterator(part.map(map))
        }
        else {
          Iterator.empty
        }
      }
    )
    new GrapeGraphImpl[VD,ED2](vertices, grapeEdges.withPartitionsRDD(newEdgePartitions))
  }

  override def mapEdges[ED2](f: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2])(implicit evidence$5: ClassTag[ED2]): Graph[VD, ED2] = {
    val newEdges = grapeEdges.mapEdgePartitions(part => part.map(f))
    new GrapeGraphImpl[VD,ED2](vertices,newEdges)
  }

  override def mapTriplets[ED2: ClassTag](map: EdgeTriplet[VD, ED] => ED2): Graph[VD, ED2] = {
    mapTriplets(map, TripletFields.All)
  }

  override def mapTriplets[ED2: ClassTag](
                                  map: EdgeTriplet[VD, ED] => ED2,
                                  tripletFields: TripletFields): Graph[VD, ED2] = {
    //broadcast outer vertex data
    //After map vertices, broadcast new inner vertex data to outer vertex data
    var newVertices = vertices
    if (!grapeVertices.outerVertexSynced){
      newVertices = grapeVertices.syncOuterVertex
    }
    else {
      logger.info(s"${grapeVertices} has done outer vertex data sync, just go to map triplets")
    }

    val newEdgePartitions = PartitionAwareZippedBaseRDD.zipPartitions(SparkContext.getOrCreate(),grapeEdges.grapePartitionsRDD, newVertices.grapePartitionsRDD){
      (eIter,vIter) => {
        if (vIter.hasNext){
          val vPart = vIter.next()
          val epart = eIter.next()
          Iterator(epart.mapTriplets(map, vPart.innerVertexData, vPart.outerVertexData, tripletFields))
        }
        else Iterator.empty
      }
    }
    val newEdges = grapeEdges.withPartitionsRDD(newEdgePartitions)
    new GrapeGraphImpl[VD,ED2](newVertices,newEdges)
  }

  override def mapTriplets[ED2](f: (PartitionID, Iterator[EdgeTriplet[VD, ED]]) => Iterator[ED2], tripletFields: TripletFields)(implicit evidence$8: ClassTag[ED2]): Graph[VD, ED2] = {
    var newVertices = vertices
    if (!grapeVertices.outerVertexSynced){
      newVertices = grapeVertices.syncOuterVertex
    }
    else {
      logger.info(s"${grapeVertices} has done outer vertex data sync, just go to map triplets")
    }
    val newEdgePartitions = PartitionAwareZippedBaseRDD.zipPartitions(SparkContext.getOrCreate(),grapeEdges.grapePartitionsRDD, newVertices.grapePartitionsRDD){
      (eIter,vIter) => {
        if (vIter.hasNext) {
          val vPart = vIter.next()
          val epart = eIter.next()
          Iterator(epart.mapTriplets(f, vPart.innerVertexData, vPart.outerVertexData, true, true))
        }
        else Iterator.empty
      }
    }
    val newEdges = grapeEdges.withPartitionsRDD(newEdgePartitions)
    new GrapeGraphImpl[VD,ED2](newVertices,newEdges)
  }

  override def reverse: Graph[VD, ED] = {
    new GrapeGraphImpl(vertices, grapeEdges.reverse)
  }

  override def subgraph(epred: EdgeTriplet[VD, ED] => Boolean, vpred: (VertexId, VD) => Boolean): Graph[VD, ED] = {
    var newVertices = vertices
    if (!grapeVertices.outerVertexSynced){
      newVertices = grapeVertices.syncOuterVertex
    }
    else {
      logger.info(s"${grapeVertices} has done outer vertex data sync, just go to map triplets")
    }
    newVertices = newVertices.mapGrapeVertexPartitions(_.filter(vpred))

    val newEdgePartitions = PartitionAwareZippedBaseRDD.zipPartitions(SparkContext.getOrCreate(),grapeEdges.grapePartitionsRDD, newVertices.grapePartitionsRDD){
      (eIter,vIter) => {
        if (vIter.hasNext) {
          val vPart = vIter.next()
          val ePart = eIter.next()
          Iterator(ePart.filter(epred, vpred, vPart.innerVertexData,vPart.outerVertexData))
        }
        else Iterator.empty
      }
    }
    val newEdges = grapeEdges.withPartitionsRDD(newEdgePartitions)
    new GrapeGraphImpl(newVertices, newEdges)
  }

  override def mask[VD2 : ClassTag, ED2 : ClassTag](other: Graph[VD2, ED2]): Graph[VD, ED] = {
    val newVertices = grapeVertices.innerJoin(other.asInstanceOf[GrapeGraphImpl[VD,ED]].grapeVertices){ (oid, v1, v2)=> v1}
    val newEdges = grapeEdges.innerJoin(other.asInstanceOf[GrapeGraphImpl[VD,ED]].grapeEdges){ (src,dst,e1,e2) => e1}
    new GrapeGraphImpl(newVertices,newEdges)
  }

  override def groupEdges(merge: (ED, ED) => ED): Graph[VD, ED] = {
    new GrapeGraphImpl(vertices, grapeEdges.withPartitionsRDD(
      grapeEdges.grapePartitionsRDD.mapPartitions(iter => {
        if (iter.hasNext) {
          val part = iter.next()
          Iterator(part.groupEdges(merge))
        }
        else Iterator.empty
      })
    ))
  }

  override private[graphx] def aggregateMessagesWithActiveSet[A: ClassTag](
                                          sendMsg: EdgeContext[VD, ED, A] => Unit,
                                          mergeMsg: (A, A) => A,
                                          tripletFields: TripletFields,
                                          activeSetOpt: Option[(VertexRDD[_], EdgeDirection)]) : VertexRDD[A] = {
    vertices.cache()
    val newVertices = activeSetOpt match {
      case Some((activeSet, _)) => {
        throw new IllegalStateException("Currently not supported aggregate with active vertex set")
      }
      case None => vertices.syncOuterVertex
    }

    val activeDirection = activeSetOpt.map(_._2)
    val preAgg = grapeEdges.grapePartitionsRDD.zipPartitions(newVertices.grapePartitionsRDD){(eiter,viter) => {
      val epart = eiter.next()
      val vpart = viter.next()
      epart.scanEdgeTriplet(vpart.innerVertexData, vpart.outerVertexData,sendMsg,mergeMsg, tripletFields, activeDirection)
    }}.setName("GraphImpl.aggregateMessages - preAgg")

    newVertices.aggregateUsingIndex(preAgg, mergeMsg)
  }

  override def outerJoinVertices[U : ClassTag, VD2 : ClassTag](other: RDD[(VertexId, U)])(mapFunc: (VertexId, VD, Option[U]) => VD2)(implicit eq: VD =:= VD2 = null): Graph[VD2, ED] = {
    val newVertices = vertices.leftJoin(other)(mapFunc)
    new GrapeGraphImpl[VD2,ED](newVertices,edges)
  }

  def generateDegreeRDD(edgeDirection: EdgeDirection) : GrapeVertexRDD[Int] = {
      val newVertexPartitionRDD = PartitionAwareZippedBaseRDD.zipPartitions(SparkContext.getOrCreate(), grapeEdges.grapePartitionsRDD, grapeVertices.grapePartitionsRDD){
      (thisIter, otherIter) => {
        if (thisIter.hasNext) {
          val ePart = thisIter.next()
          val otherVPart = otherIter.next()
          //VertexPartition id range should be same with edge partition
          val newVdArray = ePart.getDegreeArray(edgeDirection)
          val newValues = otherVPart.innerVertexData.create[Int](newVdArray)
          require(otherVPart.innerVertexData.size == newVdArray.length)
          val newVPart = otherVPart.withNewValues(newValues)
          //IN native graphx impl, the vertex with degree 0 is not returned. But we return them as well.
          //to make the result same, we set all vertices with zero degree to inactive.
          val startLid = otherVPart.startLid
          val endLid = otherVPart.endLid
          val activeSet = new BitSetWithOffset(startLid,endLid)
          activeSet.setRange(startLid, endLid)
          var i = startLid
          while (i < endLid) {
            if (newVdArray(i) == 0) {
              activeSet.unset(i)
            }
            i += 1
          }
          Iterator(newVPart.withMask(activeSet))
        }
        else Iterator.empty
      }
    }
    grapeVertices.withGrapePartitionsRDD(newVertexPartitionRDD)
  }
}


object GrapeGraphImpl extends Logging{

  def fromExistingRDDs[VD: ClassTag,ED :ClassTag](vertices: GrapeVertexRDD[VD], edges: GrapeEdgeRDD[ED]): GrapeGraphImpl[VD,ED] ={
    new GrapeGraphImpl[VD,ED](vertices, edges)
  }

  def toGraphXGraph[VD:ClassTag, ED : ClassTag](graph : Graph[VD,ED]) : Graph[VD,ED] = {
    null
  }

  def generateGraphShuffle[VD : ClassTag, ED : ClassTag](graph : GraphImpl[VD,ED], partitioner : Partitioner) = {
    graph.replicatedVertexView.upgrade(graph.vertices,includeSrc = true,includeDst = true)
    val numPartitions = graph.vertices.getNumPartitions
    val partitioner = new HashPartitioner(numPartitions)
    graph.replicatedVertexView.edges.partitionsRDD.mapPartitions(iter => {
      val (fromPid, part) = iter.next()
      val srcLids = part.localSrcIds
      val dstLids = part.localDstIds
      val edgeAttr = part.getData
      val lid2Oid = part.getLocal2Global
      val vertexAttr = part.getVertexAttrs

      val edgesNum = part.size
      require(srcLids.length == dstLids.length)
      require(edgeAttr.length == srcLids.length)
      require(srcLids.length == part.size)

      val verticesNum = lid2Oid.length
      require(lid2Oid.length == vertexAttr.length)
      log.info(s"shuffle from ${fromPid} edge num ${edgesNum}, vertices num ${verticesNum}")

      val pid2src = Array.fill(numPartitions)(new PrimitiveVector[VertexId])
      val pid2Dst = Array.fill(numPartitions)(new PrimitiveVector[VertexId])
      val pid2EdgeAttr = Array.fill(numPartitions)(new PrimitiveVector[ED])
      val pid2Oids = Array.fill(numPartitions)(new PrimitiveVector[VertexId])
      val pid2OuterOids = Array.fill(numPartitions)(new OpenHashSet[VertexId])
      val pid2VertexAttr = Array.fill(numPartitions)(new PrimitiveVector[VD])

      //shatter vertices
      var i = 0
      while (i < verticesNum){
        val dstPid = partitioner.getPartition(lid2Oid(i))
        pid2Oids(dstPid).+=(lid2Oid(i))
        pid2VertexAttr(dstPid).+=(vertexAttr(i))
        i += 1
      }

      i = 0
      while (i < edgesNum){
        val srcOid = lid2Oid(srcLids(i))
        val dstOid = lid2Oid(dstLids(i))
        val srcPid = partitioner.getPartition(srcOid)
        val dstPid = partitioner.getPartition(dstOid)
        if (srcPid == dstPid){
          pid2src(srcPid).+=(srcOid)
          pid2Dst(dstPid).+=(dstOid)
          pid2EdgeAttr(srcPid).+=(edgeAttr(i))
        }
        else {
          pid2src(srcPid).+=(srcOid)
          pid2Dst(srcPid).+=(dstOid)
          pid2EdgeAttr(srcPid).+=(edgeAttr(i))
          pid2OuterOids(srcPid).add(dstOid)
          pid2src(dstPid).+=(srcOid)
          pid2Dst(dstPid).+=(dstOid)
          pid2EdgeAttr(dstPid).+=(edgeAttr(i))
          pid2OuterOids(dstPid).add(srcOid)
        }
        i += 1
      }
      val res = new ArrayBuffer[(PartitionID,EdgeShuffle[VD,ED])]
      var ind = 0
      while (ind < numPartitions){
        log.info(s"partition ${fromPid} send msg to ${ind}")
        res.+=((ind, new EdgeShuffle(fromPid, ind, pid2Oids(ind).trim().array, pid2OuterOids(ind), pid2src(ind).trim().array, pid2Dst(ind).trim().array, pid2EdgeAttr(ind).trim().array,pid2VertexAttr(ind).trim().array)))
        ind += 1
      }
      res.toIterator
    }).partitionBy(partitioner).setName("GraphxGraph2Fragment.graphShuffle").cache()
  }
}
