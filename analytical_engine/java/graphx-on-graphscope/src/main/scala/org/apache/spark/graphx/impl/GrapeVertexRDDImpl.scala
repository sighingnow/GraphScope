package org.apache.spark.graphx.impl

import com.alibaba.graphscope.graphx.SharedMemoryRegistry
import org.apache.spark.{OneToOneDependency, Partition}
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GrapeUtils.bytesForType
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.{ClassTag, classTag}

class GrapeVertexRDDImpl[VD](
                              @transient val grapePartitionsRDD: RDD[(PartitionID, GrapeVertexPartition[VD])],
                              val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
                            (implicit override protected val vdTag: ClassTag[VD])
  extends GrapeVertexRDD[VD](grapePartitionsRDD.context, List(new OneToOneDependency(grapePartitionsRDD))) {

  override protected def getPartitions: Array[Partition] = grapePartitionsRDD.partitions
  val vdClass: Class[VD] = classTag[VD].runtimeClass.asInstanceOf[java.lang.Class[VD]]
  //  val MAPPED_SIZE : Long = 2L * 1024 * 1024 * 1024;
  //  val VERTEX_FILE_PREFIX = "/tmp/vertex-partition-"

  /**
   * Just inherit but don't use.
   *
   * @return
   */
  override def partitionsRDD = null

  override def count(): Long = {
    grapePartitionsRDD.map(_._2.innerVertexNum.toLong).fold(0)(_ + _)
  }

  override def numPartitions = grapePartitionsRDD.getNumPartitions

  override def reindex(): VertexRDD[VD] = ???

  override private[graphx] def mapVertexPartitions[VD2](f: ShippableVertexPartition[VD] => ShippableVertexPartition[VD2])(implicit evidence$1: ClassTag[VD2]) = {
    throw new IllegalStateException("Not implemented")
  }

  override private[graphx] def mapGrapeVertexPartitions[VD2: ClassTag](f: GrapeVertexPartition[VD] => GrapeVertexPartition[VD2]): GrapeVertexRDD[VD2] = {
    val newPartitionsRDD = grapePartitionsRDD.mapPartitions({
      iter => {
        if (iter.hasNext){
          val tuple = iter.next()
          val partition = tuple._2
          Iterator((tuple._1, f(partition)))
        }
        else {
          Iterator.empty
        }
      }
    }, preservesPartitioning = true)
    this.withGrapePartitionsRDD[VD2](newPartitionsRDD)
  }


  override def mapValues[VD2](f: VD => VD2)(implicit evidence$2: ClassTag[VD2]): VertexRDD[VD2] = ???

  override def mapValues[VD2](f: (VertexId, VD) => VD2)(implicit evidence$3: ClassTag[VD2]): VertexRDD[VD2] = ???

  override def minus(other: RDD[(VertexId, VD)]): VertexRDD[VD] = ???

  override def minus(other: VertexRDD[VD]): VertexRDD[VD] = ???

  override def diff(other: RDD[(VertexId, VD)]): VertexRDD[VD] = ???

  override def diff(other: VertexRDD[VD]): VertexRDD[VD] = ???

  override def leftZipJoin[VD2, VD3](other: VertexRDD[VD2])(f: (VertexId, VD, Option[VD2]) => VD3)(implicit evidence$4: ClassTag[VD2], evidence$5: ClassTag[VD3]): VertexRDD[VD3] = ???

  override def leftJoin[VD2, VD3](other: RDD[(VertexId, VD2)])(f: (VertexId, VD, Option[VD2]) => VD3)(implicit evidence$6: ClassTag[VD2], evidence$7: ClassTag[VD3]): VertexRDD[VD3] = ???

  override def innerZipJoin[U, VD2](other: VertexRDD[U])(f: (VertexId, VD, U) => VD2)(implicit evidence$8: ClassTag[U], evidence$9: ClassTag[VD2]): VertexRDD[VD2] = ???

  override def innerJoin[U, VD2](other: RDD[(VertexId, U)])(f: (VertexId, VD, U) => VD2)(implicit evidence$10: ClassTag[U], evidence$11: ClassTag[VD2]): VertexRDD[VD2] = ???

  override def aggregateUsingIndex[VD2](messages: RDD[(VertexId, VD2)], reduceFunc: (VD2, VD2) => VD2)(implicit evidence$12: ClassTag[VD2]): VertexRDD[VD2] = ???

  override def reverseRoutingTables(): VertexRDD[VD] = ???

  override def withEdges(edges: EdgeRDD[_]): VertexRDD[VD] = ???

  override def withPartitionsRDD[VD2](newPartitionsRDD: RDD[ShippableVertexPartition[VD2]])(implicit evidence$13: ClassTag[VD2]): Null = {
    throw new IllegalStateException("Not implemented");
  }

  private[graphx] def withGrapePartitionsRDD[VD2: ClassTag](
            newPartitionsRDD: RDD[(PartitionID, GrapeVertexPartition[VD2])]): GrapeVertexRDD[VD2] = {
    new GrapeVertexRDDImpl[VD2](newPartitionsRDD, targetStorageLevel)
  }


  override def withTargetStorageLevel(targetStorageLevel: StorageLevel): GrapeVertexRDDImpl[VD] = {
    new GrapeVertexRDDImpl[VD](grapePartitionsRDD, targetStorageLevel)
  }

  override def shipVertexAttributes(shipSrc: Boolean, shipDst: Boolean): RDD[(PartitionID, VertexAttributeBlock[VD])] = ???

  override private[graphx] def shipVertexIds() = {
    null
  }

  override def mapToFile(filePrefix: String, mappedSize: Long): Array[String] = {
    grapePartitionsRDD.foreachPartition({
      iter => {
        val registry = SharedMemoryRegistry.getOrCreate()
        if (iter.hasNext) {
          val tuple = iter.next()
          val pid = tuple._1
          val partition = tuple._2
          val dstFile = filePrefix + pid
          val mappedBuffer = registry.mapFor(dstFile, mappedSize)
          //          val startAddress = mappedBuffer.getAddr
          //          val bufferWriter = new MemoryMappedBufferWriter(startAddress, mappedSize)
          //Write data.
          val innerVertexNum = partition.innerVertexNum
          val innerVertexOidArray = partition.ivLid2Oid
          val vertexDataArray = partition.vdataArray
          val totalBytes = 8L + 4L + 16 + innerVertexNum * 8 + innerVertexNum * bytesForType(vdClass)
          log.info("Total bytes written: " + totalBytes)
          //First put header
          //| 8bytes    | 4Bytes   | 8bytes  | ...... | 8Bytes   | .....
          //| total-len | vd type  | oid len |        | data len |
          mappedBuffer.writeLong(totalBytes)
          mappedBuffer.writeInt(GrapeUtils.class2Int(vdClass))
          mappedBuffer.writeLong(8L * innerVertexNum)

          var ind = 0
          while (ind < innerVertexNum) {
            mappedBuffer.writeLong(innerVertexOidArray(ind))
            ind += 1
          }
          log.info(s"Partition: ${pid} Finish writing oid array of size ${innerVertexNum} to ${dstFile}")

          mappedBuffer.writeLong(innerVertexNum.toLong * bytesForType[VD](vdClass))

          ind = 0
          if (vdClass.equals(classOf[Long])) {
            while (ind < innerVertexNum) {
              mappedBuffer.writeLong(vertexDataArray(ind).asInstanceOf[Long])
              ind += 1
            }
          }
          else if (vdClass.equals(classOf[Double])) {
            while (ind < innerVertexNum) {
              mappedBuffer.writeDouble(vertexDataArray(ind).asInstanceOf[Double])
              ind += 1
            }
          }
          else if (vdClass.equals(classOf[Int])) {
            while (ind < innerVertexNum) {
              mappedBuffer.writeInt(vertexDataArray(ind).asInstanceOf[Int])
              ind += 1
            }
          }
          else {
            throw new IllegalStateException("Unsupported vd type: " + vdClass.getName)
          }
          log.info(s"Partition: ${pid} Finish writing vdata array of size ${innerVertexNum} to ${dstFile}")
        }
      }
    })
    val mappedFileSet = grapePartitionsRDD.mapPartitions({
      iter => {
        val registry = SharedMemoryRegistry.getOrCreate()
        Iterator(registry.getAllMappedFileNames(filePrefix))
      }
    })
    log.info("collect mappedFileSet: " + mappedFileSet.collect().mkString("Array(", ", ", ")"))
    mappedFileSet.collect()
  }

  override def createMapFilePerExecutor(filepath: String, mappedSize: VertexId): Unit = {
    grapePartitionsRDD.foreachPartition( iter => {
      val registry = SharedMemoryRegistry.getOrCreate()
      if (iter.hasNext) {
        val tuple = iter.next()
        val mappedBuffer = registry.tryMapFor(filepath, mappedSize)
      }
    })
  }

  override def updateVertexData(filePath: String, mappedSize: VertexId): Unit = {
    grapePartitionsRDD.foreachPartition(
      iter =>{
        val registry = SharedMemoryRegistry.getOrCreate()
        if (iter.hasNext){
          val tuple = iter.next()
          val vertexPartition = tuple._2
          val mappedBuffer = registry.tryMapFor(filePath, mappedSize)
          val length = mappedBuffer.readLong(0);
          log.info(s"There are total ${length} bytes can be read")
          var ind = 8
          if (vdClass.equals(classOf[Long])){
            while (ind + 16 <= length){
              val oid = mappedBuffer.readLong(ind)
              val data = mappedBuffer.readLong(ind + 8)
              vertexPartition.updateData(oid, data.asInstanceOf[VD])
              ind = ind + 16
            }
            if (ind + 16 != length){
              throw new IllegalStateException("length should be equal to 16 * vertices" + length)
            }
          }
          else if (vdClass.equals(classOf[Int])){
            while (ind + 12 <= length){
              val oid = mappedBuffer.readLong(ind)
              val data = mappedBuffer.readInt(ind + 8)
              vertexPartition.updateData(oid, data.asInstanceOf[VD])
              ind = ind + 12
            }
            if (ind + 12 != length){
              throw new IllegalStateException("length should be equal to 12 * vertices" + length)
            }
          }
          else if (vdClass.equals(classOf[Double])){
            while (ind + 16 <= length){
              val oid = mappedBuffer.readLong(ind)
              val data = mappedBuffer.readInt(ind + 8)
              vertexPartition.updateData(oid, data.asInstanceOf[VD])
              ind = ind + 16
            }
            if (ind + 16 != length){
              throw new IllegalStateException("length should be equal to 12 * vertices" + length)
            }
          }
        }
      }
    )
  }

//  override private[graphx] def withGrapePartitionsRDD[VD2: ClassTag](partitionsRDD: RDD[(PartitionID, GrapeVertexPartition[VD2])]) = ???
}
