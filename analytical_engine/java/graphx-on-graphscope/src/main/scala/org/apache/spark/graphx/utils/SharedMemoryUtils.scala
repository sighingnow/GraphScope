package org.apache.spark.graphx.utils

import com.alibaba.graphscope.graphx.SharedMemoryRegistry
import com.alibaba.graphscope.utils.MappedBuffer
import org.apache.spark.graphx.impl.{EdgePartition, GrapeUtils}
import org.apache.spark.graphx.impl.GrapeUtils.{bytesForType, getMethodFromClass}
import org.apache.spark.graphx.{EdgeRDD, PartitionID, VertexRDD}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

import java.io.{BufferedWriter, File, FileWriter}
import scala.reflect.{ClassTag, classTag}

object SharedMemoryUtils extends Logging{
  def mapVerticesToFile[VD: ClassTag](vertices : VertexRDD[VD], vprefix : String, mappedSize : Long): Array[String] = {
    val vdClass = classTag[VD].runtimeClass.asInstanceOf[java.lang.Class[VD]]

    vertices.partitionsRDD.zipWithIndex().foreachPartition(
      iter =>{
        val registry = SharedMemoryRegistry.getOrCreate()
        if (iter.hasNext){
          val tuple = iter.next()
          val partition = tuple._1
          val pid = tuple._2
          val bufferedWriter = new BufferedWriter(new FileWriter(new File("/tmp/graphx-log-" + pid)))
          val innerVerticesNum = partition.size

          val dstFile = vprefix + pid
          val mappedBuffer = registry.mapFor(dstFile, mappedSize)

          val totalBytes = 8L + 4L + 16 + innerVerticesNum * 8 + innerVerticesNum * bytesForType(vdClass)
          bufferedWriter.write("Total bytes written: " + totalBytes + "mapped size: " + mappedSize  +"\n")
          bufferedWriter.write("ivnum: " + innerVerticesNum)
          //First put header
          //| 8bytes    | 4Bytes   | 8bytes  | ...... | 8Bytes   | .....
          //| total-len | vd type  | oid len |        | data len |
          mappedBuffer.writeLong(totalBytes)
          mappedBuffer.writeInt(GrapeUtils.class2Int(vdClass))
          mappedBuffer.writeLong(8L * innerVerticesNum)
          val vertexIter = partition.iterator
          val vidArray = new Array[Long](innerVerticesNum)
          val vdataArray = new Array[VD](innerVerticesNum)
          require(vidArray.length == vdataArray.length)
          var ind = 0;
          while (vertexIter.hasNext){
            val value = vertexIter.next()
            vidArray(ind) = value._1
            vdataArray(ind) = value._2
            ind += 1
          }
          writeVertices(mappedBuffer, vidArray, vdataArray, innerVerticesNum, vdClass, bufferedWriter)
        }
      }
    )
    val mappedFileSet = vertices.partitionsRDD.mapPartitions({
      iter => {
        val registry = SharedMemoryRegistry.getOrCreate()
        Iterator(registry.getAllMappedFileNames(vprefix))
      }
    })
    log.info("collect mappedFileSet: " + mappedFileSet.collect().mkString("Array(", ", ", ")"))
    mappedFileSet.collect()
  }
  def mapEdgesToFile[ED: ClassTag](edges : EdgeRDD[ED], eprefix : String, mappedSize : Long): Array[String] = {
    val edClass = classTag[ED].runtimeClass.asInstanceOf[java.lang.Class[ED]]
    edges.partitionsRDD.foreachPartition(
      iter => {
        val registry = SharedMemoryRegistry.getOrCreate()
        if (iter.hasNext){
          val tuple = iter.next()
          val pid = tuple._1
          val partition = tuple._2
          val edgesNum = partition.size

          val dstFile = eprefix + pid
          val mappedBuffer = registry.mapFor(dstFile, mappedSize)
          val totalBytes = 8L + 4L + 16 + edgesNum * 16 + edgesNum * bytesForType(edClass)
          //First put header
          //| 8bytes    | 4Bytes   | 8bytes     | ......    | ......     |   8Bytes   | .....
          //| total-len | ed type  | srcOid len |  srcoids  |  dstOids   |   edata len |
          mappedBuffer.writeLong(totalBytes)
          mappedBuffer.writeInt(GrapeUtils.class2Int(edClass))
          mappedBuffer.writeLong(8 * edgesNum.toLong)

          val srcIds = new Array[Long](edgesNum)
          val dstIds = new Array[Long](edgesNum)
          val attrs = new Array[ED](edgesNum)
          var ind = 0
          val partitionIter = partition.iterator
          while (ind < edgesNum && partitionIter.hasNext){
            val edge = partitionIter.next()
            srcIds(ind) = edge.srcId
            dstIds(ind) = edge.dstId
            attrs(ind) = edge.attr
            ind += 1
          }
          ind = 0
          while (ind < edgesNum) {
            mappedBuffer.writeLong(srcIds(ind))
            ind += 1
          }
          ind = 0
          while (ind < edgesNum) {
            mappedBuffer.writeLong(dstIds(ind))
            ind += 1
          }
          mappedBuffer.writeLong(edgesNum.toLong * bytesForType[ED](edClass))

          ind = 0
          if (edClass.equals(classOf[Long])) {
            while (ind < edgesNum) {
              mappedBuffer.writeLong(attrs(ind).asInstanceOf[Long])
              ind += 1
            }
          }
          else if (edClass.equals(classOf[Double])) {
            while (ind < edgesNum) {
              mappedBuffer.writeDouble(attrs(ind).asInstanceOf[Double])
              ind += 1
            }
          }
          else if (edClass.equals(classOf[Int])) {
            while (ind < edgesNum) {
              mappedBuffer.writeInt(attrs(ind).asInstanceOf[Int])
              ind += 1
            }
          }
          else {
            throw new IllegalStateException("Unsupported vd type: " + edClass.getName)
          }
          log.info(s"Partition: ${pid} Finish writing vdata array of size ${edgesNum} to ${dstFile}")
        }
      }
    )
    val mappedFileSet = edges.partitionsRDD.mapPartitions({
      iter => {
        val registry = SharedMemoryRegistry.getOrCreate()
        Iterator(registry.getAllMappedFileNames(eprefix))
      }
    })
    val mappedFileArray = mappedFileSet.collect()
    log.info("collect mappedFileSet: " + mappedFileSet.collect().mkString("Array(", ", ", ")"))
    mappedFileArray
  }

  def mapEdgePartitionToFile[ED: ClassTag, VD : ClassTag](edgePartition : RDD[(PartitionID, EdgePartition[ED,VD])],
                                                          eprefix : String, size : Long): Array[String] = {
    val edClass = classTag[ED].runtimeClass.asInstanceOf[java.lang.Class[ED]]
    edgePartition.foreachPartition(
      iter => {
        val registry = SharedMemoryRegistry.getOrCreate()
        if (iter.hasNext){
          val tuple = iter.next()
          val pid = tuple._1
          val partition = tuple._2
          val edgesNum = partition.size

          val dstFile = eprefix + pid
          val totalBytes = 8L  + edgesNum * 16  + edgesNum * bytesForType(edClass)
          val mappedBuffer = registry.mapFor(dstFile, size)
          //First put header
          //| 8bytes    | ......    | ......     | .....    |
          //| total-len |  srcoids  |  dstOids   |   edatas |
          mappedBuffer.writeLong(edgesNum)

          val srcIds = new Array[Long](edgesNum)
          val dstIds = new Array[Long](edgesNum)
          val attrs = new Array[ED](edgesNum)
          var ind = 0
          val partitionIter = partition.iterator
          while (ind < edgesNum && partitionIter.hasNext){
            val edge = partitionIter.next()
            srcIds(ind) = edge.srcId
            dstIds(ind) = edge.dstId
            attrs(ind) = edge.attr
            ind += 1
          }
          ind = 0
          while (ind < edgesNum) {
            mappedBuffer.writeLong(srcIds(ind))
            ind += 1
          }
          ind = 0
          while (ind < edgesNum) {
            mappedBuffer.writeLong(dstIds(ind))
            ind += 1
          }

          ind = 0
          if (edClass.equals(classOf[Long])) {
            while (ind < edgesNum) {
              mappedBuffer.writeLong(attrs(ind).asInstanceOf[Long])
              ind += 1
            }
          }
          else if (edClass.equals(classOf[Double])) {
            while (ind < edgesNum) {
              mappedBuffer.writeDouble(attrs(ind).asInstanceOf[Double])
              ind += 1
            }
          }
          else if (edClass.equals(classOf[Int])) {
            while (ind < edgesNum) {
              mappedBuffer.writeInt(attrs(ind).asInstanceOf[Int])
              ind += 1
            }
          }
          else {
            throw new IllegalStateException("Unsupported vd type: " + edClass.getName)
          }
          log.info(s"Partition: ${pid} Finish writing edges of size ${edgesNum} to ${dstFile}, total Bytes ${totalBytes}")
        }
      }
    )
    val mappedFileSet = edgePartition.mapPartitions({
      iter => {
        val registry = SharedMemoryRegistry.getOrCreate()
        Iterator(registry.getAllMappedFileNames(eprefix))
      }
    })
    val mappedFileArray = mappedFileSet.collect()
    log.info("collect mappedFileSet: " + mappedFileSet.collect().mkString("Array(", ", ", ")"))
    mappedFileArray
  }

  def writeVertices[VD : ClassTag](buffer: MappedBuffer, vidArray: Array[Long], attrs: Array[VD],
                                   size: Int, vdClass : Class[VD], bufferedWriter : BufferedWriter): Unit ={
    var ind = 0
    while (ind < size) {
      buffer.writeLong(vidArray(ind))
      ind += 1
    }
    buffer.writeLong(size.toLong * bytesForType[VD](vdClass))
    bufferedWriter.write("Start writing vdata: " + buffer.position() + " limit: " + buffer.remaining() + " size: " + size + "\n")

    ind = 0
    if (vdClass.equals(classOf[Long])) {
      while (ind < size) {
        buffer.writeLong(attrs(ind).asInstanceOf[Long])
        bufferedWriter.write("write " + attrs(ind) + ",ind " + ind + " size: " + size + "\n")
        ind += 1
      }
    }
    else if (vdClass.equals(classOf[Double])) {
      while (ind < size) {
        buffer.writeDouble(attrs(ind).asInstanceOf[Double])
        ind += 1
      }
    }
    else if (vdClass.equals(classOf[Int])) {
      while (ind < size) {
        buffer.writeInt(attrs(ind).asInstanceOf[Int])
        ind += 1
      }
    }
    else {
      throw new IllegalStateException("Unsupported vd type: " + vdClass.getName)
    }
  }
}
