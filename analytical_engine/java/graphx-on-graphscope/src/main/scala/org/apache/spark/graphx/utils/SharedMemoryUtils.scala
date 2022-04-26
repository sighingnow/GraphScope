package org.apache.spark.graphx.utils

import org.apache.spark.graphx.{EdgeRDD, VertexRDD}

import scala.reflect.ClassTag

object SharedMemoryUtils {
  def mapVerticesToFile[VD: ClassTag](vertices : VertexRDD[VD], vprefix : String, mappedSize : Long) = {

  }
  def mapEdgesToFile[VD: ClassTag](vertices : EdgeRDD[VD], vprefix : String, mappedSize : Long) = {

  }
}
