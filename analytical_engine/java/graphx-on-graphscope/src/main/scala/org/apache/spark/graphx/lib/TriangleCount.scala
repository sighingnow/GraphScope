package org.apache.spark.graphx.lib

import org.apache.spark.graphx.TypeAlias.PrimitiveVector

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.util.collection.OpenHashSet

object TriangleCount extends Logging{

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[Int, ED] = {

    val triangleGraph = graph.mapVertices((vid,vd) =>(0, new OpenHashSet[VertexId], -1))

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
        require(msg.size == 1)
        log.info(s"vertex ${id} in round 2, got res ${msg(0)}")
        (3, attr._2,msg(0).toInt)
      }
      else attr
    }


    def sendMessage(edge: EdgeTriplet[(Int, OpenHashSet[Long],Int), ED]) : Iterator[(VertexId,PrimitiveVector[VertexId])] = {
      if (edge.srcAttr._1 != edge.dstAttr._1){
        throw new IllegalStateException("Not possible")
      }
      val stage = edge.srcAttr._1
      if (stage == 0){
        val a = new PrimitiveVector[VertexId](1)
        val b = new PrimitiveVector[VertexId](1)
        a.+=(edge.dstId)
        b.+=(edge.srcId)
        Iterator((edge.srcId, a),(edge.dstId,b))
      }
      else if (stage == 1){
        val size = (edge.srcAttr._2.getBitSet & edge.dstAttr._2.getBitSet).cardinality()
        log.info(s"join ${edge.srcAttr._2.getBitSet.cardinality()} with ${edge.dstAttr._2.getBitSet.cardinality()} got ${size}")
        val a = new PrimitiveVector[VertexId](1)
        a.+=(size)
        Iterator((edge.srcId, a), (edge.dstId,a))
      }
      else {
        Iterator.empty
      }
    }

    def messageCombiner(a: PrimitiveVector[VertexId], b: PrimitiveVector[VertexId]): PrimitiveVector[VertexId] = {
      require(b.size == 1)
      a.+=(b(0))
      a
    }

    val initialMsg = new PrimitiveVector[VertexId](1) //empty msg
    val res = triangleGraph.pregel(initialMsg)(vertexProgram, sendMessage, messageCombiner)
    res.mapVertices((vid,vd) => vd._3)
  }
}

