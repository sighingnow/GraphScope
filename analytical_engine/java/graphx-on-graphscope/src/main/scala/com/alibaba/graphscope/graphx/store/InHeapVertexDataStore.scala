package com.alibaba.graphscope.graphx.store

import com.alibaba.graphscope.graphx.VineyardClient
import com.alibaba.graphscope.graphx.utils.GrapeUtils
import org.apache.spark.internal.Logging

import java.util.concurrent.atomic.AtomicInteger
import scala.reflect.ClassTag

class InHeapVertexDataStore[@specialized(Long,Double,Int) VD: ClassTag](val offset : Int, val length : Int, val client : VineyardClient, var numSplit : Int, val outer : Boolean, val vdArray : Array[VD]) extends VertexDataStore [VD] with Logging {

  def this(offset : Int, length : Int, client : VineyardClient, numSplit : Int, outer : Boolean = false) = {
    this(offset,length,client, numSplit, outer,new Array[VD](length))
  }
  var vertexDataV6dId: Long = 0L
  var resultArray : InHeapVertexDataStore[_] = null.asInstanceOf[InHeapVertexDataStore[_]]
  override def size: Int = vdArray.length
  val count = new AtomicInteger(numSplit)

  def setNumSplit(split : Int) : Unit = {
    this.numSplit = split
    count.set(split)
  }

  @inline
  override def getData(lid: Int): VD = vdArray(lid - offset)

  override def vineyardID: Long = {
    if (vertexDataV6dId == 0) {
      //FIXME. merge array to one.
      vertexDataV6dId = GrapeUtils.array2ArrowArray[VD](vdArray, client,true)
    }
    vertexDataV6dId
  }

  @inline
  override def setData(lid: Int, vd: VD): Unit = vdArray(lid - offset) = vd

  /** create a new store from current, all the same except for vertex data type */
//  override def create[VD2: ClassTag]: VertexDataStore[VD2] = new InHeapVertexDataStore[VD2](offset, length, client)

  override def getOrCreate[VD2: ClassTag]: VertexDataStore[VD2] = synchronized{
    if (resultArray == null || count.get() == 0){
      synchronized {
        log.info(s"creating result array of type ${GrapeUtils.getRuntimeClass[VD2].getSimpleName}")
        resultArray = new InHeapVertexDataStore[VD2](offset, length, client, numSplit, outer).asInstanceOf[InHeapVertexDataStore[_]]
        count.set(numSplit)
      }
    }
  log.info(s"using already exiting res array ${resultArray}")
  count.decrementAndGet()
  resultArray.asInstanceOf[VertexDataStore[VD2]]
  }

//  override def create[VD2: ClassTag](newArr: Array[VD2]): VertexDataStore[VD2] = new InHeapVertexDataStore[VD2](offset,length,client,newArr)

  override def toString: String = {
    val res = if (outer) "Outer" else "Inner"
    res + "InHeapVertexDataStore@(offset=" + offset + ",length=" + length + ",type=" + GrapeUtils.getRuntimeClass[VD].getSimpleName + ")"
  }
}

