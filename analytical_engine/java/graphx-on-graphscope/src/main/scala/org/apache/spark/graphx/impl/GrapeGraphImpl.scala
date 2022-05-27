package org.apache.spark.graphx.impl

import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder
import com.alibaba.graphscope.graphx.VertexDataBuilder
import org.apache.spark.HashPartitioner
import org.apache.spark.graphx.impl.grape.{GrapeEdgeRDDImpl, GrapeVertexRDDImpl}
import org.apache.spark.graphx.utils.{ExecutorUtils, ScalaFFIFactory}
import org.slf4j.{Logger, LoggerFactory}
//import com.alibaba.graphscope.utils.FragmentRegistry
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.{ClassTag, classTag}

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

  protected def this(vertices : GrapeVertexRDDImpl[VD], edges : GrapeEdgeRDDImpl[VD,ED], fragId : String) =
    this(vertices.asInstanceOf[GrapeVertexRDD[VD]],edges.asInstanceOf[GrapeEdgeRDD[ED]])

  val vdClass: Class[VD] = classTag[VD].runtimeClass.asInstanceOf[java.lang.Class[VD]]
  val edClass: Class[ED] = classTag[ED].runtimeClass.asInstanceOf[java.lang.Class[ED]]
  val grapeEdges: GrapeEdgeRDDImpl[VD, ED] = edges.asInstanceOf[GrapeEdgeRDDImpl[VD,ED]]
  val grapeVertices: GrapeVertexRDDImpl[VD] = vertices.asInstanceOf[GrapeVertexRDDImpl[VD]]

  def numVertices: Long = grapeVertices.count()

  def numEdges: Long = edges.count()

  def generateGlobalVMIds() : Array[String] = {
    edges.grapePartitionsRDD.mapPartitions(iter => {
      if (iter.hasNext){
        val tuple = iter.next()
        Iterator(ExecutorUtils.getHostName + ":" + tuple._1 +":" + tuple._2.vm.id())
      }
      else {
        Iterator.empty
      }
    }).collect()
  }

  def generateCSRIds() : Array[String] = {
    edges.grapePartitionsRDD.mapPartitions(iter => {
      if (iter.hasNext){
        val tuple = iter.next()
        val ePart = tuple._2
        val pid = tuple._1
        //build new edge data.
        //FIXME: support output the modified data to mpi processes.
        Iterator(ExecutorUtils.getHostName + ":"  + pid + ":" + ePart.csr.id())
      } else {
        Iterator.empty
      }
    }).collect()
  }

  def generateVdataIds() : Array[String] = {
    vertices.grapePartitionsRDD.mapPartitions(iter => {
      if (iter.hasNext){
        val tuple = iter.next()
        val vPart = tuple._2
        val pid = tuple._1
        Iterator(ExecutorUtils.getHostName + ":" + pid + ":" + vPart.vertexData.vineyardID)
      }
      else {
        Iterator.empty
      }
    }).collect()
  }


  @transient override lazy val triplets: RDD[EdgeTriplet[VD, ED]] = {
    grapeEdges.grapePartitionsRDD.zipPartitions(grapeVertices.grapePartitionsRDD){
      (edgeIter, vertexIter) => {
        val (pid, edgePart) = edgeIter.next()
        val (_, vertexPart) = vertexIter.next()
        edgePart.tripletIterator(vertexPart.vertexData)
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
          val (pid,part) = iter.next()
          Iterator((pid,part.map(map)))
        }
        else {
          Iterator.empty
        }
      }
    )
    new GrapeGraphImpl[VD,ED2](vertices, grapeEdges.withPartitionsRDD(newEdgePartitions))
  }

  override def mapEdges[ED2](f: (PartitionID, Iterator[Edge[ED]]) => Iterator[ED2])(implicit evidence$5: ClassTag[ED2]): Graph[VD, ED2] = {
    val newEdges = grapeEdges.mapEdgePartitions((pid,part) => part.map(f(pid, part.iterator)))
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
      logger.info(s"${grapeVertices} is doing outer vertex data sync")
      val updateMessage = grapeVertices.grapePartitionsRDD.mapPartitions(iter => {
        if (iter.hasNext){
          val tuple = iter.next()
          val pid = tuple._1
          val part = tuple._2
          part.generateVertexDataMessage
        }
        else {
          Iterator.empty
        }
      }).partitionBy(new HashPartitioner(grapeVertices.grapePartitionsRDD.getNumPartitions))
      val updatedVertexPartition = grapeVertices.grapePartitionsRDD.zipPartitions(updateMessage){
        (vIter, msgIter) => {
          val (pid, vpart) = vIter.next()
          Iterator((pid,vpart.updateOuterVertexData(msgIter)))
        }
      }.cache()
      newVertices = grapeVertices.withGrapePartitionsRDD(updatedVertexPartition, true)
    }
    else {
      logger.info(s"${grapeVertices} has done outer vertex data sync, just go to map triplets")
    }

    val newEdgePartitions = grapeEdges.grapePartitionsRDD.zipPartitions(newVertices.grapePartitionsRDD){
      (eIter,vIter) => {
        val (pid, vPart) = vIter.next()
        val (_, epart) = eIter.next()
        Iterator((pid, epart.mapTriplets(map, vPart.vertexData, tripletFields)))
      }
    }
    val newEdges = grapeEdges.withPartitionsRDD(newEdgePartitions)
    new GrapeGraphImpl[VD,ED2](newVertices,newEdges)
  }

  override def mapTriplets[ED2](f: (PartitionID, Iterator[EdgeTriplet[VD, ED]]) => Iterator[ED2], tripletFields: TripletFields)(implicit evidence$8: ClassTag[ED2]): Graph[VD, ED2] = {
    val newEdgePartitions = grapeEdges.grapePartitionsRDD.zipPartitions(grapeVertices.grapePartitionsRDD){
      (eIter,vIter) => {
        val (pid, vPart) = vIter.next()
        val (_, epart) = eIter.next()
        Iterator((pid, epart.map(f(pid, epart.tripletIterator(vPart.vertexData, tripletFields.useSrc, tripletFields.useDst)))))
      }
    }
    val newEdges = grapeEdges.withPartitionsRDD(newEdgePartitions)
    new GrapeGraphImpl[VD,ED2](grapeVertices,newEdges)
  }

  override def reverse: Graph[VD, ED] = {
    new GrapeGraphImpl(vertices, grapeEdges.reverse)
  }

  override def subgraph(epred: EdgeTriplet[VD, ED] => Boolean, vpred: (VertexId, VD) => Boolean): Graph[VD, ED] = {
    val newVertices = grapeVertices.mapGrapeVertexPartitions(_.filter(vpred))
    val newEdgePartitions = grapeEdges.grapePartitionsRDD.zipPartitions(newVertices.grapePartitionsRDD){
      (eIter,vIter) => {
        val (pid, vPart) = vIter.next()
        val (_, ePart) = eIter.next()
        Iterator((pid, ePart.filter(epred,vpred, vPart.vertexData)))
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
        val (pid, part) = iter.next()
        Iterator((pid, part.groupEdges(merge)))
      })
    ))
  }

  override private[graphx] def aggregateMessagesWithActiveSet[A: ClassTag](sendMsg: EdgeContext[VD, ED, A] => Unit, mergeMsg: (A, A) => A, tripletFields: TripletFields, activeSetOpt: Option[(VertexRDD[_], EdgeDirection)]) : GrapeVertexRDD[A] = {
    throw new IllegalStateException("Not implemented")
  }

  override def outerJoinVertices[U : ClassTag, VD2 : ClassTag](other: RDD[(VertexId, U)])(mapFunc: (VertexId, VD, Option[U]) => VD2)(implicit eq: VD =:= VD2 = null): Graph[VD2, ED] = {
    val newVertices = vertices.leftJoin(other)(mapFunc)
    new GrapeGraphImpl[VD2,ED](newVertices,edges)
  }
}


object GrapeGraphImpl {

  def fromExistingRDDs[VD: ClassTag,ED :ClassTag](vertices: GrapeVertexRDDImpl[VD], edges: GrapeEdgeRDDImpl[VD, ED]): GrapeGraphImpl[VD,ED] ={
    new GrapeGraphImpl[VD,ED](vertices, edges)
  }

  def toGraphXGraph[VD:ClassTag, ED : ClassTag](graph : Graph[VD,ED]) : Graph[VD,ED] = {
    null
  }

  def fromRDDs[VD: ClassTag, ED : ClassTag](vertices : VertexRDD[VD], edges : EdgeRDD[ED], fragId :String) : GrapeGraphImpl[VD,ED] = {
    /** it is possible that the vd type bound with edges has changes in this operation, but edge rdd just casted */
    new GrapeGraphImpl[VD,ED](vertices.asInstanceOf[GrapeVertexRDDImpl[VD]], edges.asInstanceOf[GrapeEdgeRDDImpl[VD,ED]], fragId);
  }

}
