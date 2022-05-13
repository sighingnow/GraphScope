package org.apache.spark.graphx.impl.partition

import com.alibaba.graphscope.graphx.GraphXVertexMap

import scala.reflect.ClassTag

class GrapeVertexPartition[VD : ClassTag](val pid : Int, val startLid : Long, val endLid : Long,
                                          val vm : GraphXVertexMap[Long,Long]){
  def partVnum : Long = endLid - startLid

}
