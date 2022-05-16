package org.apache.spark.graphx

import com.alibaba.graphscope.fragment.IFragment
import com.alibaba.graphscope.graphx.{GSEdgeTriplet, GSEdgeTripletImpl, SerializationUtils}
import com.alibaba.graphscope.parallel.DefaultMessageManager
import com.alibaba.graphscope.utils.MappedBuffer
import org.apache.spark.graphx.impl.graph.GraphXProxy
import org.apache.spark.graphx.impl.graph.{EdgeManagerImpl, GraphXProxy, GraphXVertexIdManagerImpl, VertexDataManagerImpl}
import org.apache.spark.graphx.impl.message.DefaultMessageStore
import org.apache.spark.graphx.traits.{GraphXVertexIdManager, VertexDataManager}
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

object GraphXFactory extends Logging{

  def createGraphxConf[VD :ClassTag,ED : ClassTag, MSG: ClassTag]: GraphXConf[VD, ED, MSG] = {
    new GraphXConf[VD,ED, MSG]
  }



}
