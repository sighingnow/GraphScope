package org.apache.spark.graphx

import org.apache.spark.graphx.impl.GrapeUtils
import org.apache.spark.graphx.impl.graph.VertexDataManagerImpl
import org.apache.spark.graphx.traits.VertexDataManager
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

object VertexDataManagerCreator extends Logging{

  private var newVDManager : VertexDataManager[_] = null.asInstanceOf[VertexDataManager[_]]

  def create[VD1 : ClassTag, VD2 : ClassTag, ED : ClassTag](pid : Int, original : VertexDataManager[VD1]) : Unit = {
    if (newVDManager != null){
      throw new IllegalStateException("Expect new vd is null to perform creation")
    }
    require(!GrapeUtils.getRuntimeClass[VD1].equals(GrapeUtils.getRuntimeClass[VD2]), s"shouldn't be equals ${GrapeUtils.getRuntimeClass[VD1]}, ${GrapeUtils.getRuntimeClass[VD2]}")

    if (newVDManager == null) {
      synchronized {
        if (newVDManager == null) {
          newVDManager = new VertexDataManagerImpl[VD2,ED](new GraphXConf[VD2,ED],null,original.asInstanceOf[VertexDataManagerImpl[VD1,ED]].fragment,null)
          log.info(s"Partition [${pid}] create new VDManager: ${newVDManager}")
        }
      }
    }
    else {
      log.info(s"Partition [${pid}] use the already created one ${newVDManager}")
    }
  }

  def get[VD: ClassTag] : VertexDataManager[VD] = {
    require(newVDManager.getVDClz.equals(GrapeUtils.getRuntimeClass[VD]), s"expect class is same ${newVDManager.getVDClz}, ${GrapeUtils.getRuntimeClass[VD].getName}")
    require(newVDManager != null, "create first")
    newVDManager.asInstanceOf[VertexDataManager[VD]]
  }

  def reset() : Unit = {
    newVDManager = null
  }
}
