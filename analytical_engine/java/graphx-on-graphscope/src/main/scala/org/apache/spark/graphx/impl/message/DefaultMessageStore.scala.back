package org.apache.spark.graphx.impl.message

import com.alibaba.graphscope.ds.Vertex
import com.alibaba.graphscope.fragment.{ArrowProjectedFragment, IFragment}
import com.alibaba.graphscope.parallel.DefaultMessageManager
import com.alibaba.graphscope.utils.FFITypeFactoryhelper
import com.alibaba.graphscope.utils.array.PrimitiveArray
import org.apache.spark.graphx.GraphXConf
import org.apache.spark.graphx.impl.GrapeUtils
import org.apache.spark.graphx.traits.{GraphXVertexIdManager, MessageStore, VertexDataManager}
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

class DefaultMessageStore[VD: ClassTag, MSG: ClassTag](val conf : GraphXConf[VD,_],
                                                       val fragment : IFragment[Long,Long,_,_],
                                                       val idManager: GraphXVertexIdManager,
                                                       val vertexDataManager: VertexDataManager[VD],
                                                       val mergeMsg : (MSG,MSG) => MSG) extends Logging with MessageStore[MSG]{
  val ivNum = fragment.getInnerVerticesNum.toInt
  val fragVNum = fragment.getVerticesNum.toInt
  var bitSet = new java.util.BitSet(fragVNum)
  var values = PrimitiveArray.create(GrapeUtils.getRuntimeClass[MSG], fragVNum).asInstanceOf[PrimitiveArray[MSG]]
  log.info(s"Create default message store size ${fragVNum} value array ${values}")
  val vertex = FFITypeFactoryhelper.newVertexLong().asInstanceOf[Vertex[java.lang.Long]]
  val projectedFragment = fragment.getFFIPointer.asInstanceOf[ArrowProjectedFragment[Long, Long, _, _]]

  override def messageAvailable(lid: Long): Boolean = {
    bitSet.get(lid.toInt)
  }

  override def hasMessages: Boolean = {
    !bitSet.isEmpty
  }

  override def getMessage(lid: Long): MSG = {
    values.get(lid.toInt)
  }

  override def addLidMessage(lid: Long, msg: MSG): Unit = {
    val intLid = lid.toInt
    if (bitSet.get(intLid)){
      log.info(s"merge msg at ${intLid}, prev ${values.get(intLid)}, incoming ${msg}")
      values.set(intLid, mergeMsg.apply(values.get(intLid), msg))
    }
    else {
      log.info(s"put first msg at ${intLid} msg ${msg}")
      bitSet.set(intLid)
      values.set(intLid, msg)
    }
  }

  override def addOidMessage(oid: Long, msg: MSG): Unit = {
    addLidMessage(idManager.oid2Lid(oid), msg)
  }

  override def clear(): Unit = {
    bitSet.clear()
  }

  override def swap(messageStore: MessageStore[MSG]): Unit = {
    if (messageStore.isInstanceOf[DefaultMessageStore[VD, MSG]]) {
      val other = messageStore.asInstanceOf[DefaultMessageStore[VD, MSG]]
      //only swap flags and values are ok
      log.info(s"frag ${fragment.fid} Before message store swap ${this.bitSet.cardinality} vs ${other.bitSet.cardinality}")
      val tmp = other.bitSet
      other.bitSet = this.bitSet
      this.bitSet = tmp
      log.info(s"frag ${fragment.fid} After message store swap ${this.bitSet.cardinality} vs ${other.bitSet.cardinality}")
      val tmpValues = other.values
      other.values = this.values
      this.values = tmpValues
    }
    else {
      throw new IllegalStateException(s"${this} can not swap with ${messageStore}")
    }
  }

  override def flushMessage(messageManager: DefaultMessageManager): Unit = {
    var index = bitSet.nextSetBit(ivNum)
    var msgCnt = 0
    while (index >= ivNum && index < fragVNum && index >= 0) {
      if (index == Integer.MAX_VALUE) throw new IllegalStateException("Overflow is not expected")
      vertex.SetValue(index.toLong)
      messageManager.syncStateOnOuterVertexArrowProjected(projectedFragment, vertex, values.get(index))
      //CAUTION-------------------------------------------------------------
      //update outer vertices data here, otherwise will cause infinite message sending
      //FIXME
      vertexDataManager.setVertexData(index, values.get(index).asInstanceOf[VD])
      index = bitSet.nextSetBit(index + 1)
      msgCnt += 1
    }
    bitSet.clear(ivNum, fragVNum)
    log.info("frag [{}] send msg of size {}", fragment.fid, msgCnt)
  }
}
