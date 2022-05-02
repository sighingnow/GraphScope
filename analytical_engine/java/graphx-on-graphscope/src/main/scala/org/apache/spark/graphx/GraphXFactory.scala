package org.apache.spark.graphx

import com.alibaba.graphscope.fragment.IFragment
import com.alibaba.graphscope.graphx.{GSEdgeTriplet, GSEdgeTripletImpl, SerializationUtils}
import com.alibaba.graphscope.parallel.DefaultMessageManager
import org.apache.spark.graphx.impl.graph.GraphXProxy
import org.apache.spark.graphx.impl.graph.{EdgeManagerImpl, GraphXProxy, GraphXVertexIdManagerImpl, VertexDataManagerImpl}
import org.apache.spark.graphx.impl.message.DefaultMessageStore
import org.apache.spark.graphx.traits.{GraphXVertexIdManager, VertexDataManager}
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

object GraphXFactory extends Logging{

  def createGraphxConf[VD :ClassTag,ED : ClassTag]: GraphXConf[VD, ED] = {
    new GraphXConf[VD,ED]
  }

  def createGraphxProxy[VD : ClassTag,ED : ClassTag,MSG: ClassTag](conf : GraphXConf[VD,ED], iFragment: IFragment[Long,Long,_,_], mm : DefaultMessageManager, vprogPath : String,
                                   sendMsgFilePath : String, mergeMsgFilePath : String, vdataFilePath : String, maxIter: Int,
                                   numCores : Int, vdataSize : Long, initialmessage : MSG): GraphXProxy[VD,ED,MSG] ={

    val vprog: (VertexId,VD,MSG)=>VD = deserializeVprog(vprogPath, conf)
    val sendMsg: (EdgeTriplet[VD,ED]=>Iterator[(VertexId,MSG)]) = deserializeSendMsg(sendMsgFilePath, conf)
    val mergeMsg: (MSG,MSG)=>MSG = deserializeMergeMsg(mergeMsgFilePath, conf)
    log.info(s"deserialization success: ${vprog}, ${sendMsg}, ${mergeMsg}")

    new GraphXProxy[VD,ED,MSG](conf, iFragment, mm, initialmessage,vprog, sendMsg, mergeMsg, maxIter, numCores, vdataFilePath, vdataSize)
  }

  def createVertexIdManager[VD,ED](conf: GraphXConf[VD,ED], fragment : IFragment[Long,Long,_,_]): GraphXVertexIdManager ={
    new GraphXVertexIdManagerImpl(conf, fragment)
  }

  def createVertexDataManager[VD: ClassTag, ED : ClassTag](conf: GraphXConf[VD, ED],  fragment : IFragment[Long,Long,_,_], values : Array[VD] = null,defaultOutData : VD = null.asInstanceOf[VD]): VertexDataManager[VD] = {
    new VertexDataManagerImpl[VD,ED](conf, values, fragment, defaultOutData)
  }

  def createDefaultMessageStore[VD : ClassTag,MSG : ClassTag](conf: GraphXConf[_,_], fragment : IFragment[Long,Long,_,_], idManager : GraphXVertexIdManager, vertexDataManager : VertexDataManager[VD], mergeMsg : (MSG,MSG) => MSG): DefaultMessageStore[MSG] = {
    new DefaultMessageStore[MSG]
  }
  def createEdgeTriplet[VD: ClassTag, ED : ClassTag](conf: GraphXConf[VD,ED]): GSEdgeTriplet[VD, ED] = {
    new GSEdgeTripletImpl[VD,ED]
  }

  def createEdgeManager[VD : ClassTag,ED : ClassTag](conf: GraphXConf[VD,ED], fragment : IFragment[Long,Long,_,_], vertexIdManager: GraphXVertexIdManager, vertexDataManager: VertexDataManager[VD], numCores : Int): EdgeManagerImpl[VD, ED] = {
    new EdgeManagerImpl[VD,ED](conf, fragment, vertexIdManager, vertexDataManager, numCores)
  }


  private def deserializeVprog[VD, ED, MSG](vprogFilePath: String, conf: GraphXConf[VD, ED]) : (Long,VD,MSG) => VD = {
    try {
      val res = SerializationUtils.read(vprogFilePath).asInstanceOf[(Long, VD, MSG) => VD]
      res
    } catch {
      case e: ClassNotFoundException =>
        e.printStackTrace()
        throw new IllegalStateException("deserialization vprog failed")
    }
  }

  private def deserializeSendMsg[VD, ED, MSG](sendMsgFilePath: String, conf: GraphXConf[VD, ED]) :(EdgeTriplet[VD, ED]) => Iterator[(VertexId, MSG)] = {
    try {
      val res = SerializationUtils.read(sendMsgFilePath).asInstanceOf[EdgeTriplet[VD, ED] => Iterator[(VertexId, MSG)]]
      res
    } catch {
      case e: ClassNotFoundException =>
        e.printStackTrace()
        throw new IllegalStateException("deserialization send msg failed")
    }
  }

  private def deserializeMergeMsg[VD, ED, MSG](mergeMsgFilePath: String, conf: GraphXConf[VD, ED]): (MSG,MSG) => MSG = {
    try {
      val res = SerializationUtils.read(mergeMsgFilePath).asInstanceOf[(MSG, MSG) => MSG]
      res
    } catch {
      case e: ClassNotFoundException =>
        e.printStackTrace()
        throw new IllegalStateException("deserialization merge msg failed")
    }
  }
}
