package org.apache.spark.graphx.impl.graph

import com.alibaba.graphscope.fragment.IFragment
import com.alibaba.graphscope.graph.AbstractEdgeManager
import com.alibaba.graphscope.graphx.{GSEdgeTriplet, GSEdgeTripletImpl, ReverseGSEdgeTripletImpl}
import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.spark.util.collection.BitSet
import org.apache.spark.graphx.{Edge, EdgeTriplet, GraphXConf, ReusableEdge, ReusableEdgeImpl, ReversedReusableEdge, TripletFields, VertexId}
import org.apache.spark.graphx.traits.{EdgeManager, GraphXVertexIdManager, MessageStore, VertexDataManager}
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

class EdgeManagerImpl[VD: ClassTag,ED : ClassTag](var conf: GraphXConf[VD,ED],
                                                  var vertexIdManager: GraphXVertexIdManager,
                                                  var vertexDataManager: VertexDataManager[VD],
                                                  var dstOids : PrimitiveArray[Long],
                                                  var dstLids : PrimitiveArray[Long],
                                                  var nbrPositions : Array[Int],
                                                  var numOfEdges : Array[Long],
                                                  var edatas : PrimitiveArray[ED],
                                                  var edataOffset :Int,
                                                  var edgeReversed: Boolean,
                                                  var activeSet : BitSet)
  extends AbstractEdgeManager[Long,Long,Long,ED,ED]() with EdgeManager[VD,ED]{

  private val logger = LoggerFactory.getLogger(classOf[EdgeManagerImpl[_,_]].getName)

  def this() = {
    this(null,null,null,null,null,null,null,null,0,false,null)
  }

  def this(inConf: GraphXConf[VD,ED], inFragment : IFragment[Long,Long,_,_], inVertexIdManager: GraphXVertexIdManager,
           inVertexDataManager: VertexDataManager[VD], inNumCores : Int,
           inEdatas : PrimitiveArray[ED] = null, inEdataOffset :Int = 0,inEdgeReversed: Boolean = false) = {
    this()
    super[AbstractEdgeManager].init(inFragment.asInstanceOf[IFragment[Long,Long,_,ED]], inVertexIdManager, classOf[java.lang.Long].asInstanceOf[Class[_ <: Long]], classOf[java.lang.Long].asInstanceOf[Class[_ <: Long]], inConf.getEdClass, inConf.getEdClass, null, inNumCores)
    val tmpDstOids: PrimitiveArray[Long] = csrHolder.dstOids
    val tmpDstLids: PrimitiveArray[Long] = csrHolder.dstLids
    val tmpNbrPositions: Array[Int] = csrHolder.nbrPositions
    val tmpNumOfEdges: Array[Long] = csrHolder.numOfEdges
    var realEdatas = null.asInstanceOf[PrimitiveArray[ED]]
    if (inEdatas == null || inEdatas.size() == 0) {
      logger.info("No edata provided, read from csr");
      realEdatas = csrHolder.edatas
    }
    val inactiveSet = new BitSet(tmpDstLids.size())
    initialize(inConf, inVertexIdManager, inVertexDataManager, tmpDstOids, tmpDstLids, tmpNbrPositions, tmpNumOfEdges, realEdatas, inEdataOffset, inEdgeReversed,inactiveSet)
    //    this(conf, vertexIdManager, vertexDataManager,dstOids, dstLids, nbrPositions, numOfEdges, realEdatas, edataOffset, edgeReversed)
  }

  def initialize(inConf: GraphXConf[VD, ED], manager: GraphXVertexIdManager, vdManager: VertexDataManager[VD], oids: PrimitiveArray[Long], lids: PrimitiveArray[Long],
                 nbrPos: Array[Int], numEdges: Array[Long], inEdatas: PrimitiveArray[ED], inEdataOffset: Int, bool: Boolean, set: BitSet): Unit ={
    this.conf = inConf;
    this.vertexIdManager = manager
    this.vertexDataManager = vdManager
    this.dstOids = oids
    this.dstLids = lids
    this.nbrPositions = nbrPos
    this.numOfEdges = numEdges
    this.edatas = inEdatas
    this.edataOffset = inEdataOffset
    this.edgeReversed = bool
    this.activeSet = set
  }
  if (activeSet == null){
    activeSet = new BitSet(dstLids.size())
    activeSet.setUntil(dstLids.size())
  }
  logger.info(s"Using customized edata, length ${edatas.size()}, offset ${edataOffset}");
  require(edataOffset < getTotalEdgeNum, s"offset error ${edataOffset} greater than ${getTotalEdgeNum}")
  require(edatas.size() < getTotalEdgeNum, s"length ${edatas.size()} should smaller than ${getTotalEdgeNum}")
  logger.info(s"create EdgeManagerImpl(${this}), reversed ${edgeReversed}")


  override def iterator(startLid: Long, endLid: Long): Iterator[Edge[ED]] = {
    new Iterator[Edge[ED]]() {
      private var curLid = startLid
      private var edge : ReusableEdge[ED] = null.asInstanceOf[ReusableEdge[ED]]
      if (edgeReversed){
        edge = new ReusableEdgeImpl[ED]
      }
      else {
        edge = new ReversedReusableEdge[ED]
      }
      var numEdge: Long = numOfEdges(curLid.toInt)
      var nbrPos: Int = nbrPositions(curLid.toInt)
      var endPos: Int = (nbrPos + numEdge).toInt
      var curPos: Int = nbrPos

      def hasNext: Boolean = {
        //logger.info("has next: curLId {} endLid {} curPos {} endPos {} numEdge {}", curLid, endLid, curPos, endPos, numEdge);
        while (activeSet.get(curPos)){
          curPos += 1
        }
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

  override def tripletIterator(startLid: Long, endLid: Long, tripletFields: TripletFields = TripletFields.All): Iterator[EdgeTriplet[VD,ED]] = {
    new Iterator[EdgeTriplet[VD,ED]]() {
      private var curLid = startLid
      private var edge : GSEdgeTriplet[VD,ED] = null.asInstanceOf[GSEdgeTriplet[VD,ED]]
      if (edgeReversed){
        edge = new GSEdgeTripletImpl[VD,ED]
      }
      else {
        edge = new ReverseGSEdgeTripletImpl[VD,ED]
      }
      var numEdge: Long = numOfEdges(curLid.toInt)
      var nbrPos: Int = nbrPositions(curLid.toInt)
      var endPos: Int = (nbrPos + numEdge).toInt
      var curPos: Int = nbrPos

      def hasNext: Boolean = {
        while (activeSet.get(curPos)) {
          curPos += 1
        }
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
          if (tripletFields.useSrc){
            edge.setSrcOid(vertexIdManager.lid2Oid(curLid), vertexDataManager.getVertexData(curLid))
          }
          else {
            edge.setSrcOid(vertexIdManager.lid2Oid(curLid))
          }
          true
        }
      }

      def next: EdgeTriplet[VD,ED] = {
        if (tripletFields.useDst){
          edge.setDstOid(dstOids.get(curPos),vertexDataManager.getVertexData(dstLids.get(curPos)))
        }
        else {
          edge.setDstOid(dstOids.get(curPos))
        }
        edge.setAttr(edatas.get(curPos - edataOffset))
        curPos += 1
        edge
      }
    }
  }

  override def getPartialEdgeNum(startLid: Long, endLid: Long): Long = {
    val startLidPos = nbrPositions(startLid.toInt)
    val endLidPos = nbrPositions(endLid.toInt - 1)
    numOfEdges(endLid.toInt) + endLidPos - startLidPos
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
      if (!activeSet.get(i)) {
        triplet.setDstOid(dstOids.get(i), vertexDataManager.getVertexData(dstLids.get(i)))
        triplet.setAttr(edatas.get(i - edataOffset))
        val iterator = msgSender.apply(triplet)
        logger.info("for edge: {}->{}", triplet.srcId, triplet.dstId)
        while (iterator.hasNext) {
          val tuple2 = iterator.next
          outMessageCache.addOidMessage(tuple2._1, tuple2._2)
        }
      }
      i += 1
    }
  }

  override def withNewEdgeData[ED2 : ClassTag](newEdgeData: PrimitiveArray[ED2], startLid: Long, endLid: Long): EdgeManager[VD, ED2] = {
    val newEdataOffset = getPartialEdgeNum(0, startLid)
    require(newEdataOffset + newEdgeData.size() == getPartialEdgeNum(startLid, endLid),
      s"override edata array size not match ${newEdataOffset + newEdgeData.size()} should match ${getPartialEdgeNum(startLid,endLid)}")
    new EdgeManagerImpl[VD,ED2](new GraphXConf[VD,ED2], vertexIdManager, vertexDataManager, dstOids, dstLids, nbrPositions, numOfEdges, newEdgeData, edataOffset, edgeReversed, activeSet)
  }

  override def toString: String = "EdgeManagerImpl(length=" + dstLids.size() + ",numEdges=" + getTotalEdgeNum+ ")"

  /**
   * Reverse src,dst pairs. return a new edgeManager.
   * This reverse will not write back to c++ memory.
   * For ease of implementation, we only reverse iterators. we don't really reverse edges.
   *
   * @param startLid start vid
   * @param endLid   end vid
   */
  override def reverseEdges(): EdgeManager[VD,ED] = {
    new EdgeManagerImpl[VD,ED](conf, vertexIdManager, vertexDataManager, dstOids, dstLids, nbrPositions, numOfEdges, edatas, edataOffset, !edgeReversed, activeSet)
  }

  /**
   * Return a new edge manager, will only partial of the original data.
   *
   * @param epred
   * @param vpred
   * @return
   */
  override def filter(epred: EdgeTriplet[VD, ED] => Boolean, vpred: (VertexId, VD) => Boolean, startLid : Long, endLid : Long): EdgeManager[VD, ED] = {
    if (activeSet == null){
      throw new IllegalStateException("Not possible")
    }
    val iter = tripletIterator(startLid, endLid)
    var ind = 0;
    while (iter.hasNext){
      val triplet = iter.next()
      if (epred(triplet) || vpred(triplet.srcId, triplet.srcAttr) || vpred(triplet.dstId,triplet.dstAttr)){
        activeSet.set(ind)
      }
      ind += 1
    }
    new EdgeManagerImpl[VD,ED](conf, vertexIdManager, vertexDataManager,dstOids, dstLids, nbrPositions, numOfEdges, edatas, edataOffset, !edgeReversed,activeSet)
  }
}
