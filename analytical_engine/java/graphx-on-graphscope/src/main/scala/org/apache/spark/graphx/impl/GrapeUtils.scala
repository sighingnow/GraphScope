package org.apache.spark.graphx.impl

import java.lang.reflect.Method
import scala.reflect.ClassTag

object GrapeUtils {

  def class2Int(value: Class[_]): Int = {
    if (value.equals(classOf[java.lang.Long]) || value.equals(classOf[Long])) {
      4
    }
    else if (value.equals(classOf[java.lang.Integer]) || value.equals(classOf[Int])) {
      2
    }
    else if (value.equals(classOf[java.lang.Double]) || value.eq(classOf[Double])) {
      7
    }
    else throw new IllegalArgumentException(s"unexpected class ${value}")
  }
  def bytesForType[VD: ClassTag](value: Class[VD]): Int ={
    if (value.equals(classOf[Long]) || value.equals(classOf[Double])) 8
    else if (value.eq(classOf[Int]) || value.equals(classOf[Float])) 4
    else throw new IllegalStateException("Unrecognized : " + value.getName)
  }

  def classToStr(value : Class[_]) : String = {
    if (value.equals(classOf[java.lang.Long]) || value.equals(classOf[Long])) {
      "int64_t"
    }
    else if (value.equals(classOf[java.lang.Integer]) || value.equals(classOf[Int])) {
      "int32_t"
    }
    else if (value.equals(classOf[java.lang.Double]) || value.eq(classOf[Double])) {
      "double"
    }
    else throw new IllegalArgumentException(s"unexpected class ${value}")
  }

  def getMethodFromClass[T](clz : Class[T], name : String ,paramClasses : Class[_]) : Method = {
    val method = clz.getDeclaredMethod(name, paramClasses)
    method.setAccessible(true)
    require(method != null, "can not find method: " + name)
    method
  }

  def generateForeignFragName[VD: ClassTag, ED : ClassTag](vdClass : Class[VD], edClass : Class[ED]): String ={
    val sb = StringBuilder
    sb.+("gs::ArrowProjectedFragment<int64_t,uint64_t")
    sb.+(classToStr(vdClass))
    sb.+(",")
    sb.+(classToStr(edClass))
    sb.+(">")
    sb.toString
  }
}
