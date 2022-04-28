package org.apache.spark.graphx.impl.graph

import com.alibaba.graphscope.fragment.IFragment
import com.alibaba.graphscope.graph.AbstractEdgeManager
import com.alibaba.graphscope.graphx.GSEdgeTriplet
import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.spark.graphx.{Edge, EdgeTriplet, GraphXConf, VertexId}
import org.apache.spark.graphx.traits.{EdgeManager, GraphXVertexIdManager, MessageStore, VertexDataManager}
import org.slf4j.LoggerFactory
import org.apache.spark.graphx.ReusableEdge

import scala.reflect.ClassTag

class EdgeManagerImpl[VD: ClassTag,ED : ClassTag](
                              conf: GraphXConf[VD,ED], fragment : IFragment[Long,Long,_,_],
                              vertexIdManager: GraphXVertexIdManager, vertexDataManager: VertexDataManager[VD], numCores : Int, var edatas : PrimitiveArray[ED] = null,
                              val edataOffset :Int = 0) extends AbstractEdgeManager[Long,Long,Long,ED,ED] with EdgeManager[VD,ED]{
  private val logger = LoggerFactory.getLogger(classOf[EdgeManagerImpl[_,_]].getName)
  super.init(fragment.asInstanceOf[IFragment[Long,Long,_,ED]], vertexIdManager, classOf[java.lang.Long].asInstanceOf[Class[_ <: Long]], classOf[java.lang.Long].asInstanceOf[Class[_ <: Long]], conf.getEdClass, conf.getEdClass, null, numCores)
  val dstOids: PrimitiveArray[Long] = csrHolder.dstOids
  val dstLids: PrimitiveArray[Long] = csrHolder.dstLids
  val nbrPositions: Array[Int] = csrHolder.nbrPositions
  val numOfEdges: Array[Long] = csrHolder.numOfEdges
  if (edatas == null || edatas.size() == 0){
    logger.info("No edata provided, read from csr");
    edatas = csrHolder.edatas
  }
  else {
    logger.info(s"Using customized edata, length ${edatas.size()}, offset ${edataOffset}");
    require(edataOffset < getTotalEdgeNum, s"offset error ${edataOffset} greater than ${getTotalEdgeNum}")
    require(edatas.size() < getTotalEdgeNum, s"length ${edatas.size()} should smaller than ${getTotalEdgeNum}")
  }

  logger.info(s"create EdgeManagerImpl(${this})")


  override def iterator(startLid: Long, endLid: Long): Iterator[Edge[ED]] = {
    new Iterator[Edge[ED]]() {
      private var curLid = startLid
      private val edge = new ReusableEdge[ED]
      var numEdge: Long = numOfEdges(curLid.toInt)
      var nbrPos: Int = nbrPositions(curLid.toInt)
      var endPos: Int = (nbrPos + numEdge).toInt
      var curPos: Int = nbrPos

      def hasNext: Boolean = {
        //logger.info("has next: curLId {} endLid {} curPos {} endPos {} numEdge {}", curLid, endLid, curPos, endPos, numEdge);
        if (curLid >= endLid) return false
        if (curPos < endPos) true
        else {
          curLid += 1
          numEdge = numOfEdges(curLid.toInt)
          while (curLid < endLid && numEdge <= 0){
            curLid += 1
            numEdge = numOfEdges(curLid.toInt)
          }
          if (curLid >= endLid) return false
          nbrPos = nbrPositions(curLid.toInt)
          endPos = (nbrPos + numEdge).toInt
          curPos = nbrPos
          //logger.info("has next move to new lid: curLId {} endLid {} curPos {} endPos {} numEdge {}", curLid, endLid, curPos, endPos, numEdge);
          edge.setSrcId(vertexIdManager.lid2Oid(curLid))
          true
        }
      }

      def next: Edge[ED] = {
        edge.setDstId(dstOids.get(curPos))
        edge.setAttr(edatas.get(curPos - edataOffset))
        //	logger.info("src{}, dst{}}", dstOids[curPos], edatas[curPos]);
        curPos += 1
        edge
      }
    }
  }

  override def getPartialEdgeNum(startLid: Long, endLid: Long): Long = {
    val startLidPos = nbrPositions(startLid.toInt)
    val endLidPos = nbrPositions(endLid.toInt - 1)
    numOfEdges(endLid.toInt - 1) + endLidPos - startLidPos
  }

  override def getTotalEdgeNum: Long = {
    dstLids.size()
  }

  override def iterateOnEdgesParallel[MSG](tid: Int, srcLid: Long, triplet: GSEdgeTriplet[VD, ED], msgSender: EdgeTriplet[VD, ED] => Iterator[(VertexId, MSG)], outMessageCache: MessageStore[MSG]): Unit = {
    val numEdge = numOfEdges(srcLid.toInt)
    val nbrPos = nbrPositions(srcLid.toInt)
    val endPos = (nbrPos + numEdge).toInt
    var i = nbrPos
    while (i < endPos) {
      triplet.setDstOid(dstOids.get(i), vertexDataManager.getVertexData(dstLids.get(i)), edatas.get(i - edataOffset))
      val iterator = msgSender.apply(triplet)
      logger.info("for edge: {}->{}", triplet.srcId, triplet.dstId)
      while (iterator.hasNext) {
        val tuple2 = iterator.next
        outMessageCache.addOidMessage(tuple2._1, tuple2._2)
      }
      i += 1
    }
  }

  override def withNewEdgeData[ED2 : ClassTag](newEdgeData: PrimitiveArray[ED2], startLid: Long, endLid: Long): EdgeManager[VD, ED2] = {
    val newEdataOffset = getPartialEdgeNum(0, startLid)
    require(newEdataOffset + newEdgeData.size() == getPartialEdgeNum(startLid, endLid),
      s"override edata array size not match ${newEdataOffset + newEdgeData.size()} should match ${getPartialEdgeNum(startLid,endLid)}")
    new EdgeManagerImpl[VD,ED2](new GraphXConf[VD,ED2], fragment, vertexIdManager, vertexDataManager, numCores, newEdgeData, newEdataOffset.toInt)
  }

  override def toString: String = "EdgeManagerImpl(length=" + dstLids.size() + ",numEdges=" + getTotalEdgeNum+ ")"
}
