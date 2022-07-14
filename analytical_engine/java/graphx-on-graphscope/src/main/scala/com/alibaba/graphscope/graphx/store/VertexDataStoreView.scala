package com.alibaba.graphscope.graphx.store

import scala.reflect.ClassTag

class VertexDataStoreView[@specialized(Long,Double,Int) VD: ClassTag](val vertexDataStore: VertexDataStore[VD], val startLid : Int, val endLid : Int) extends VertexDataStore[VD] {
  override def size: Int = endLid - startLid

  override def getData(lid: Int): VD = vertexDataStore.getData(lid)

  override def setData(lid: Int, vd: VD): Unit = vertexDataStore.setData(lid,vd)

  override def vineyardID: Long = vertexDataStore.vineyardID

//  override def create[VD2: ClassTag](newArr: Array[VD2]): VertexDataStore[VD2] = ???

  override def getOrCreate[VD2: ClassTag]: VertexDataStore[VD2] = {
    val res = vertexDataStore.getOrCreate[VD2]
    new VertexDataStoreView[VD2](res, startLid, endLid)
  }

  /** set the created result array to null, for accepting new transformations. */
  override def clearCreatedArray(): Unit = vertexDataStore.clearCreatedArray()
}
