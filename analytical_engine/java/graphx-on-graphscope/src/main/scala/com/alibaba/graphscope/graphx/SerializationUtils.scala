package com.alibaba.graphscope.graphx

import org.apache.spark.graphx.VertexId
import org.slf4j.{Logger, LoggerFactory}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

class SerializationUtils[VD,ED,A]{
  //provide some demo functions to test
  val vprog : (VD,ED,A) => VD  = {
    (vd, ed ,a) => vd
  }
}
object SerializationUtils2 extends Serializable {
  val resetProb = 0.6
  val vprog : (VertexId, Double,Double) => Double = {
    (vid, vd, a) => {
      val innerResetProb = resetProb
      vd + (1.0 - innerResetProb) * a
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
  def read(filepath : String): Any = {
    logger.info("Reading from file path: " + filepath)
    new ObjectInputStream(new FileInputStream(new File(filepath))).readObject
  }
}
