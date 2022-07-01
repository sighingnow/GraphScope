package com.alibaba.graphscope.graphx.utils

import org.apache.spark.internal.Logging

import java.io._

/**
 * Serialize a function obj to a path;
 * deserialize a function obj from path
 */
object SerializationUtils extends Logging{
  def write[A](path : String, objs: Any*): Unit = {
    log.info("Write obj " + objs.mkString("Array(", ", ", ")") + " to :" +  path)
    val bo = new FileOutputStream(new File(path))
    val outputStream = new ObjectOutputStream(bo)
    outputStream.writeInt(objs.length)
    var i = 0
    while (i < objs.length){
      log.info(s"writing object ${objs(i)}")
      if (objs(i).equals(classOf[Long])){
        outputStream.writeObject(classOf[java.lang.Long])
      }
      else if (objs(i).equals(classOf[Int])){
        outputStream.writeObject(classOf[java.lang.Integer])
      }
      else if (objs(i).equals(classOf[Double])){
        outputStream.writeObject(classOf[java.lang.Double])
      }
      else {
        outputStream.writeObject(objs(i))
      }
      i += 1
    }
    outputStream.flush()
    outputStream.close()
  }

  @throws[ClassNotFoundException]
  def read(classLoader: ClassLoader,filepath : String): Array[Any] = {
    log.info("Reading from file path: " + filepath + ", with class loader: " + classLoader)
    val objectInputStream = new ObjectInputStream(new FileInputStream(new File(filepath))){
      @throws[IOException]
      @throws[ClassNotFoundException]
      protected override def resolveClass(desc: ObjectStreamClass): Class[_] = {
//        val cl = Thread.currentThread.getContextClassLoader
//        if (cl == null) return super.resolveClass(desc)
        log.info(s"Resolving class for ${desc}, ${desc.getName}")
        log.info(s"str eq ${desc.getName}, ${desc.getName.equals("long")}")
        if (desc.getName.equals("long")){
          classOf[java.lang.Long]
        }
        else if (desc.getName.equals("double")){
          classOf[java.lang.Double]
        }
        else if (desc.getName.equals("int")){
          classOf[java.lang.Integer]
        }
        else {
          Class.forName(desc.getName, false, classLoader)
        }
      }
    }
    val len = objectInputStream.readInt()
    val res = new Array[Any](len)
    for (i <- 0 until len){
      res(i) = objectInputStream.readObject()
    }
    res
  }
}