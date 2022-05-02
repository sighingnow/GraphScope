package org.apache.spark.graphx.impl

import java.lang.reflect.Method
import java.net.{InetAddress, UnknownHostException}
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
    if (value.equals(classOf[Long]) || value.equals(classOf[Double]) || value.equals(classOf[java.lang.Long]) || value.equals(classOf[java.lang.Double])) 8
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
    new StringBuilder().+("gs::ArrowProjectedFragment<int64_t,uint64_t,").+(classToStr(vdClass)).+(",").+(classToStr(edClass)).+(">")
  }

  def scalaClass2JavaClass[T: ClassTag](vdClass : Class[T]) : Class[_] = {
    if (vdClass.equals(classOf[Int])) {
      classOf[Integer];
    }
    else if (vdClass.equals(classOf[Long])) {
      classOf[java.lang.Long]
    }
    else if (vdClass.equals(classOf[Double])) {
      classOf[java.lang.Double]
    }
    else {
      throw new IllegalStateException("transform failed for " + vdClass.getName);
    }
  }

  def getRuntimeClass[T: ClassTag] = implicitly[ClassTag[T]].runtimeClass

  @throws[UnknownHostException]
  def getSelfHostName = InetAddress.getLocalHost.getHostName
}
