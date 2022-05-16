package com.alibaba.graphscope.graphx

import org.apache.spark.graphx.{EdgeTriplet, VertexId}
import org.slf4j.{Logger, LoggerFactory}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File, FileInputStream, FileOutputStream, IOException, ObjectInputStream, ObjectOutputStream, ObjectStreamClass}

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
  def write[A](obj: A, path : String): Unit = {
    logger.info("Write obj {} to path {}: ", obj, path)
    val bo = new FileOutputStream(new File(path))
    new ObjectOutputStream(bo).writeObject(obj)
  }

  @throws[ClassNotFoundException]
  def read(classLoader: ClassLoader,filepath : String): Any = {
    logger.info("Reading from file path: " + filepath + ", with class loader: " + classLoader)
    val objectInputStream = new ObjectInputStream(new FileInputStream(new File(filepath))){
      @throws[IOException]
      @throws[ClassNotFoundException]
      protected override def resolveClass(desc: ObjectStreamClass): Class[_] = {
//        val cl = Thread.currentThread.getContextClassLoader
//        if (cl == null) return super.resolveClass(desc)
        Class.forName(desc.getName, false, classLoader)
      }
    }
    objectInputStream.readObject()
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
