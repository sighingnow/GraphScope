package org.apache.spark.graphx.lib

//import org.apache.spark.graphx.TypeAlias.PrimitiveVector

import com.alibaba.graphscope.graphx.utils.PrimitiveVector

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.OpenHashSet

object TriangleCount extends Logging with Serializable{
  var stage = 0

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[Int, ED] = {

    val tmp = graph.outerJoinVertices(graph.collectNeighborIds(edgeDirection = EdgeDirection.Either))((vid, vd, nbrIds) => nbrIds.get)

    log.info(s"collected nbrids ${tmp.vertices.collect().mkString(",")}")
    val triangleGraph = tmp.mapVertices((vid, vd) => (0, vd.toSet))
    stage = 0

    def vp(id : VertexId, attr : (Int, Set[VertexId]), msg : Array[VertexId]) : (Int,Set[VertexId]) = {
      if (stage == 0){
        log.info(s"vertex ${id}, stage 0")
        attr
      }
      else if (stage == 1){
        log.info(s"vertex ${id}, stage 1, receive msg ${msg.mkString(",")}")
        val prevCnt = attr._1
        var curCnt = 0
        var i = 0
        val size = msg.size
        while (i < size){
          if (attr._2.contains(msg(i))){
            curCnt += 1
          }
          i += 1
        }
        log.info(s"merge msg, prev ${prevCnt}, cur ${curCnt}")
        (prevCnt + curCnt, attr._2)
      }
      else {
        attr
      }
    }

    def sendMsg(edge: EdgeTriplet[(Int, Set[VertexId]), ED]) : Iterator[(VertexId,Array[VertexId])] = {
      if (stage == 0){
        stage = 1
        log.info(s"send msg to ${edge.dstId}, ${edge.srcAttr._2.toArray.mkString(",")}")
        Iterator((edge.dstId, edge.srcAttr._2.toArray))
      }
      else {
        Iterator.empty
      }
    }

    def mergeMsg(a: Array[VertexId], b: Array[VertexId]): Array[VertexId] = {
      val res = a ++ b
      log.info(s"Merging ${a.mkString(",")} with ${b.mkString(",")} : ${res.mkString(",")}")
      res
    }

    val initialMsg = new Array[VertexId](1)
    triangleGraph.pregel(initialMsg)(vp, sendMsg, mergeMsg).mapVertices((vid,vd) => vd._1)
  }
}

/**
 *     val triangleGraph = graph.mapVertices((vid,vd) =>(0, new OpenHashSet[VertexId], -1))

    def vertexProgram(id: VertexId, attr: (Int, OpenHashSet[Long],Int), msg: PrimitiveVector[VertexId]): (Int,OpenHashSet[Long],Int) = {
      val stage = attr._1
      if (stage == 0){
        //send message this round;
        log.info(s"vertex ${id} in round 0")
        (1,attr._2,-1)
      }
      else if (stage == 1){
        //receive nbr ids this round
        var i = 0;
        val size = msg.size
        while (i < size){
          attr._2.add(msg(i))
          i +=1
        }
        log.info(s"vertex ${id} in round 1, got nbr size ${attr._2.size}")
        (2, attr._2,-1)
      }
      else if (stage == 2){
        //the incoming vector should contain one value, and is the number of triangles
        val trimmed = msg.trim().toArray
        log.info(s"vertex ${id} in round 2, receive msg ${trimmed.mkString(",")}")
        (3, attr._2,msg(msg.size - 1).toInt)
      }
      else attr
    }


    def sendMessage(edge: EdgeTriplet[(Int, OpenHashSet[Long],Int), ED]) : Iterator[(VertexId,PrimitiveVector[VertexId])] = {
      val stage = edge.srcAttr._1
      if (stage == 1){
        val a = new PrimitiveVector[VertexId](1)
        val b = new PrimitiveVector[VertexId](1)
        a.+=(edge.dstId)
        b.+=(edge.srcId)
        Iterator((edge.srcId, a),(edge.dstId,b))
      }
      else if (stage == 2){
        //send our
//        val size = (edge.srcAttr._2.getBitSet & edge.dstAttr._2.getBitSet).cardinality()
//        log.info(s"edge (${edge.srcId}->${edge.dstId}), (${edge.srcAttr._2.size}, ${edge.dstAttr._2.size})")
//        log.info(s"join ${edge.srcAttr._2.size} with ${edge.dstAttr._2.size} got ${size}")
//        val a = new PrimitiveVector[VertexId](1)
//        a.+=(size)
//        Iterator((edge.srcId, a), (edge.dstId,a))
      }
      else {
        Iterator.empty
      }
    }

    def messageCombiner(a: PrimitiveVector[VertexId], b: PrimitiveVector[VertexId]): PrimitiveVector[VertexId] = {
      var i = 0
      while (i < b.size){
         a.+=(b(i))
         i += 1
      }
      a
    }
 */

