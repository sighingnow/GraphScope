package org.apache.spark.graphx.impl

import com.alibaba.graphscope.graphx.graph.GraphStructureTypes.GraphStructureType
import com.alibaba.graphscope.graphx.graph.impl.GraphXGraphStructure
import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.spark.HashPartitioner
import org.apache.spark.graphx.impl.GrapeUtils.getRuntimeClass
import org.apache.spark.graphx.impl.grape.{GrapeEdgeRDDImpl, GrapeVertexRDDImpl}
import org.apache.spark.graphx.utils.{ExecutorUtils, ScalaFFIFactory}
import org.slf4j.{Logger, LoggerFactory}
//import com.alibaba.graphscope.utils.FragmentRegistry
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

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
    edges.grapePartitionsRDD.zipPartitions(vertices.grapePartitionsRDD) { (edgeIter, vertexIter) => {
      val ePart = edgeIter.next()
      val vPart = vertexIter.next()
      ePart.graphStructure match {
        case casted: GraphXGraphStructure =>
          val vmId = casted.vm.id() //vm id will never change

//          val newCSR = mapOldCSRToNewCSR(casted.csr, edataBuilder, ePart.client)
          val csrId = casted.csr.id()

          val vdId = vPart.vertexData.vineyardID

          val nbr = casted.csr.getOEBegin(0)
          val initAddress = nbr.getAddress
          val oldArray = ePart.edatas
          //First swap according to eids
          var i = 0
          nbr.setAddress(initAddress - 16)
          val size = oldArray.size()
          val newArray = PrimitiveArray.create(getRuntimeClass[ED], size).asInstanceOf[PrimitiveArray[ED]]
          while (i < size){
            nbr.addV(16)
            val eid = nbr.eid()
            newArray.set(eid, oldArray.get(i))
            i += 1
          }
          val edId = GrapeUtils.array2ArrowArray[ED](newArray,ePart.client)

          var fragId = null.asInstanceOf[Long]
          if (GrapeUtils.isPrimitive[VD] && GrapeUtils.isPrimitive[ED]){
            val fragBuilder = ScalaFFIFactory.newGraphXFragmentBuilder[VD, ED](ePart.client, vmId, csrId, vdId,edId)
            fragId = fragBuilder.seal(ePart.client).get().id()
          }
          else if (GrapeUtils.isPrimitive[VD]){
            val fragBuilder = ScalaFFIFactory.newGraphXStringEDFragmentBuilder[VD](ePart.client, vmId, csrId, vdId, edId)
            fragId = fragBuilder.seal(ePart.client).get().id()
          }
          else if (GrapeUtils.isPrimitive[ED]){
            val fragBuilder = ScalaFFIFactory.newGraphXStringVDFragmentBuiler[ED](ePart.client, vmId, csrId, vdId,edId)
            fragId = fragBuilder.seal(ePart.client).get().id()
          }
          else {
            val fragBuilder = ScalaFFIFactory.newGraphXStringVEDFragmentBuilder(ePart.client, vmId, csrId,vmId, edId)
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

//  def generateCSRIds() : Array[String] = {
//    edges.grapePartitionsRDD.mapPartitions(iter => {
//      if (iter.hasNext){
//        val ePart = iter.next()
//        val pid = ePart.pid
//        //build new edge data.
//        //FIXME: support output the modified data to mpi processes.
//        ePart.graphStructure match {
//          case casted: GraphXGraphStructure =>
//            //build new edata array builder
//            val edataBuilder = ScalaFFIFactory.newArrowArrayBuilder[ED](GrapeUtils.getRuntimeClass[ED].asInstanceOf[Class[ED]])
//            val numEdges = ePart.edatas.size()
//            edataBuilder.reserve(numEdges)
//            var i = 0
//            while (i < numEdges){
//              edataBuilder.unsafeAppend(ePart.edatas.get(i))
//              i += 1
//            }
//            val newCSR = mapOldCSRToNewCSR(casted.csr, edataBuilder, ePart.client)
//            //map old csr to new csr with new edata.
//            Iterator(ExecutorUtils.getHostName + ":" + pid + ":" + newCSR.id())
//          case _ =>
//            throw new IllegalStateException("Not implemented now!")
//        }
//      } else {
//        Iterator.empty
//      }
//    }).collect()
//  }

//  def mapOldCSRToNewCSR(oldCSR: GraphXCSR[Long, _], newEdata: ArrowArrayBuilder[ED], client : VineyardClient) : GraphXCSR[Long,ED] = {
//    val oldCSRTypeParams = GenericUtils.getTypeArgumentFromInterface(oldCSR.getClass)
//    require(oldCSRTypeParams.length == 2)
//    logger.info(s"Creating csr mapper from old edata ${oldCSRTypeParams(1).getName} to new edata of type ${GrapeUtils.getRuntimeClass[ED].getName}")
//    val mapper = ScalaFFIFactory.newGraphXCSRMapper[ED](oldCSRTypeParams(1))
//    genericMap(mapper, oldCSR, newEdata, oldCSRTypeParams(1),client)
//  }

//  def genericMap[OLD_ED_T,ED_T : ClassTag](mapper: GraphXCSRMapper[Long, _, ED_T], oldCSR: GraphXCSR[Long, _], newEdata : ArrowArrayBuilder[ED_T], value2: Class[OLD_ED_T], client: VineyardClient)(implicit oldEDClassTag: ClassTag[OLD_ED_T]) : GraphXCSR[Long,ED_T] = {
//    val oldCSRCasted = oldCSR.asInstanceOf[GraphXCSR[Long,OLD_ED_T]]
//    val mapperCasted = mapper.asInstanceOf[GraphXCSRMapper[Long,OLD_ED_T,ED_T]]
//    mapperCasted.map(oldCSRCasted,newEdata,client).get()
//  }

  def generateVdataIds() : Array[String] = {
    vertices.grapePartitionsRDD.mapPartitions(iter => {
      if (iter.hasNext){
        val vPart = iter.next()
        Iterator(ExecutorUtils.getHostName + ":" + vPart.pid + ":" + vPart.vertexData.vineyardID)
      }
      else {
        Iterator.empty
      }
    }).collect()
  }


  @transient override lazy val triplets: RDD[EdgeTriplet[VD, ED]] = {
    grapeEdges.grapePartitionsRDD.zipPartitions(grapeVertices.grapePartitionsRDD){
      (edgeIter, vertexIter) => {
        val edgePart = edgeIter.next()
        val vertexPart = vertexIter.next()
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
    val newEdges = grapeEdges.mapEdgePartitions(part => part.map(f(part.pid, part.iterator)))
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
          val part = iter.next()
          part.generateVertexDataMessage
        }
        else {
          Iterator.empty
        }
      }).partitionBy(new HashPartitioner(grapeVertices.grapePartitionsRDD.getNumPartitions))
      val updatedVertexPartition = grapeVertices.grapePartitionsRDD.zipPartitions(updateMessage){
        (vIter, msgIter) => {
          val  vpart = vIter.next()
          Iterator(vpart.updateOuterVertexData(msgIter))
        }
      }.cache()
      newVertices = grapeVertices.withGrapePartitionsRDD(updatedVertexPartition, true)
    }
    else {
      logger.info(s"${grapeVertices} has done outer vertex data sync, just go to map triplets")
    }

    val newEdgePartitions = grapeEdges.grapePartitionsRDD.zipPartitions(newVertices.grapePartitionsRDD){
      (eIter,vIter) => {
        val  vPart = vIter.next()
        val epart = eIter.next()
        Iterator( epart.mapTriplets(map, vPart.vertexData, tripletFields))
      }
    }
    val newEdges = grapeEdges.withPartitionsRDD(newEdgePartitions)
    new GrapeGraphImpl[VD,ED2](newVertices,newEdges)
  }

  override def mapTriplets[ED2](f: (PartitionID, Iterator[EdgeTriplet[VD, ED]]) => Iterator[ED2], tripletFields: TripletFields)(implicit evidence$8: ClassTag[ED2]): Graph[VD, ED2] = {
    val newEdgePartitions = grapeEdges.grapePartitionsRDD.zipPartitions(grapeVertices.grapePartitionsRDD){
      (eIter,vIter) => {
        val vPart = vIter.next()
        val epart = eIter.next()
        Iterator(epart.map(f(epart.pid, epart.tripletIterator(vPart.vertexData, tripletFields.useSrc, tripletFields.useDst))))
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
        val vPart = vIter.next()
        val ePart = eIter.next()
        Iterator(ePart.filter(epred,vpred, vPart.vertexData))
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
        val part = iter.next()
        Iterator(part.groupEdges(merge))
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

  def fromExistingRDDs[VD: ClassTag,ED :ClassTag](vertices: GrapeVertexRDD[VD], edges: GrapeEdgeRDD[ED]): GrapeGraphImpl[VD,ED] ={
    new GrapeGraphImpl[VD,ED](vertices, edges)
  }

  def toGraphXGraph[VD:ClassTag, ED : ClassTag](graph : Graph[VD,ED]) : Graph[VD,ED] = {
    null
  }

}
