package org.apache.spark.graphx

import org.apache.spark.graphx.impl.GrapeUtils
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

class GraphXConf[VD: ClassTag, ED: ClassTag, MSG : ClassTag] extends Logging{
  private var vdClass = GrapeUtils.getRuntimeClass[VD].asInstanceOf[Class[VD]]
  private var edClass = GrapeUtils.getRuntimeClass[ED].asInstanceOf[Class[ED]]
  private var msgClass = GrapeUtils.getRuntimeClass[ED].asInstanceOf[Class[MSG]]
  def getVdClass: Class[VD] = vdClass;
  def getEdClass : Class[ED] = edClass
  def getMsgClass : Class[MSG] = msgClass

}
