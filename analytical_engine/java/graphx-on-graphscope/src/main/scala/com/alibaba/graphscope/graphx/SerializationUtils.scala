package com.alibaba.graphscope.graphx

import org.apache.spark.graphx.{EdgeTriplet, VertexId}
import org.slf4j.{Logger, LoggerFactory}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File, FileInputStream, FileOutputStream, IOException, ObjectInputStream, ObjectOutputStream, ObjectStreamClass}
import scala.collection.mutable.ArrayBuffer

class SerializationUtils[VD,ED,A]{
  //provide some demo functions to test
  val vprog : (VD,ED,A) => VD  = {
    (vd, ed ,a) => vd
  }
}
object SerializationUtils2 {
  val resetProb = 0.6
  object Wrapper extends Serializable {
    val innerResetProb: Double = resetProb
    val vprog : (VertexId, Double,Double) => Double = {
      (vid, vd, a) => {
        vd + (1.0 - innerResetProb) * a
      }
    }
  }

}

/**
 * Serialize a function obj to a path;
 * deserialize a function obj from path
 */
object SerializationUtils{
//  val vprog1 : (_,_,_) => _  = {
//    (vd, ed ,a) => vd
//  }
  val logger : Logger = LoggerFactory.getLogger("com.alibaba.graphscope.graphx.Serializationutils")
  def write[A](path : String, objs: Any*): Unit = {
    logger.info("Write obj " + objs.mkString("Array(", ", ", ")") + " to :" +  path)
    val bo = new FileOutputStream(new File(path))
    val outputStream = new ObjectOutputStream(bo)
    outputStream.writeInt(objs.length)
    var i = 0
    while (i < objs.length){
      logger.info(s"writing object ${objs(i)}")
      outputStream.writeObject(objs(i))
      i += 1
    }
    outputStream.flush()
    outputStream.close()
  }

  @throws[ClassNotFoundException]
  def read(classLoader: ClassLoader,filepath : String): Array[Any] = {
    logger.info("Reading from file path: " + filepath + ", with class loader: " + classLoader)
    val objectInputStream = new ObjectInputStream(new FileInputStream(new File(filepath))){
      @throws[IOException]
      @throws[ClassNotFoundException]
      protected override def resolveClass(desc: ObjectStreamClass): Class[_] = {
//        val cl = Thread.currentThread.getContextClassLoader
//        if (cl == null) return super.resolveClass(desc)
        logger.info(s"Resolving class for ${desc}")
        if (desc.getName == "long"){
          classOf[java.lang.Long]
        }
        else if (desc.getName == "double"){
          classOf[java.lang.Double]
        }
        else if (desc.getName == "int"){
          classOf[java.lang.Integer]
        }
        Class.forName(desc.getName, false, classLoader)
      }
    }
    val len = objectInputStream.readInt()
    val res = new Array[Any](len)
    for (i <- 0 until len){
      res(i) = objectInputStream.readObject()
    }
    res
  }

//  private def deserializeVprog[VD, ED, MSG](vprogFilePath: String) : (Long,VD,MSG) => VD = {
//    try {
//      val res = SerializationUtils.read(vprogFilePath).asInstanceOf[(Long, VD, MSG) => VD]
//      res
//    } catch {
//      case e: ClassNotFoundException =>
//        e.printStackTrace()
//        throw new IllegalStateException("deserialization vprog failed")
//    }
//  }

}
