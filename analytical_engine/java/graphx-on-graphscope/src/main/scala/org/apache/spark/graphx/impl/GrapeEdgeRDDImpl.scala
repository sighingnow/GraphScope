/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.graphx.impl

import com.alibaba.graphscope.ds.MemoryMappedBufferWriter
import com.alibaba.graphscope.graphx.SharedMemoryRegistry
import org.apache.spark.graphx.impl.GrapeUtils.bytesForType
import org.apache.spark.graphx.{GrapeEdgeRDD, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, OneToOneDependency}

import scala.reflect.{ClassTag, classTag}

class GrapeEdgeRDDImpl[ED: ClassTag] private[graphx] (
               @transient override val grapePartitionsRDD: RDD[(PartitionID, GrapeEdgePartition[ED])],
               val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends GrapeEdgeRDD[ED](grapePartitionsRDD.context, List(new OneToOneDependency(grapePartitionsRDD))) {

  val edClass: Class[ED] = classTag[ED].runtimeClass.asInstanceOf[java.lang.Class[ED]]
  override def setName(_name: String): this.type = {
    if (grapePartitionsRDD.name != null) {
      grapePartitionsRDD.setName(grapePartitionsRDD.name + ", " + _name)
    } else {
      grapePartitionsRDD.setName(_name)
    }
    this
  }
  setName("GrapeEdgeRDD")

  /**
   * If `grapePartitionsRDD` already has a partitioner, use it. Otherwise assume that the
   * `PartitionID`s in `grapePartitionsRDD` correspond to the actual partitions and create a new
   * partitioner that allows co-partitioning with `grapePartitionsRDD`.
   */
  override val partitioner = {
    grapePartitionsRDD.partitioner.orElse(Some(new HashPartitioner(partitions.length)))
  }
  log.info("Partitioner: " + partitioner)

  override def collect(): Array[Edge[ED]] = this.map(_.copy()).collect()

  /**
   * Persists the edge partitions at the specified storage level, ignoring any existing target
   * storage level.
   */
  override def persist(newLevel: StorageLevel): this.type = {
    grapePartitionsRDD.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = false): this.type = {
    grapePartitionsRDD.unpersist(blocking)
    this
  }

  /**
   * Persists the edge partitions using `targetStorageLevel`, which defaults to MEMORY_ONLY.
   */
  override def cache(): this.type = {
    grapePartitionsRDD.persist(targetStorageLevel)
    this
  }

  override def getStorageLevel: StorageLevel = grapePartitionsRDD.getStorageLevel

  override def checkpoint(): Unit = {
    grapePartitionsRDD.checkpoint()
  }

  override def isCheckpointed: Boolean = {
    firstParent[(VertexId, (VertexId,VertexId,ED))].isCheckpointed
  }

  override def getCheckpointFile: Option[String] = {
    grapePartitionsRDD.getCheckpointFile
  }

  /** The number of edges in the RDD. */
  override def count(): Long = {
//    grapePartitionsRDD.map(_._2.size.toLong).fold(0)(_ + _)
    grapePartitionsRDD.map(_._2.ivEdgeNum.toLong).fold(0)(_ + _)
  }

//  override def mapValues[ED2: ClassTag](f: Edge[ED] => ED2): G[ED2] =
//    mapEdgePartitions((pid, part) => part.map(f))

//  override def mapValues[ED2: ClassTag](f: Edge[ED] => ED2): GrapeEdgeRDDImpl[ED2, VD] = {
//    mapEdges(f);
////    mapEdgePartitions((pi
  // d, part) => part.map(f))
//  }


  /**
   * Map the values in an edge partitioning preserving the structure but changing the values.
   *
   * @tparam ED2 the new edge value type
   * @param f the function from an edge to a new edge value
   * @return a new EdgeRDD containing the new edge values
   */
  override def mapValues[ED2: ClassTag](f: Edge[ED] => ED2): GrapeEdgeRDDImpl[ED2] = {
    mapEdgePartitions((pid,part) => part.map(f))
  }

  /**
   * Reverse all the edges in this RDD.
   *
   * @return a new EdgeRDD containing all the edges reversed
   */
  override def reverse: EdgeRDD[ED] = {
    null
  }

  /**
   * Inner joins this EdgeRDD with another EdgeRDD, assuming both are partitioned using the same
   * [[PartitionStrategy]].
   *
   * @param other the EdgeRDD to join with
   * @param f     the join function applied to corresponding values of `this` and `other`
   * @return a new EdgeRDD containing only edges that appear in both `this` and `other`,
   *         with values supplied by `f`
   */
  override def innerJoin[ED2: ClassTag, ED3: ClassTag](other: EdgeRDD[ED2])(f: (VertexId, VertexId, ED, ED2) => ED3): EdgeRDD[ED3] = {
    null
  }

  /**
   * Changes the target storage level while preserving all other properties of the
   * EdgeRDD. Operations on the returned EdgeRDD will preserve this storage level.
   *
   * This does not actually trigger a cache; to do this, call
   * [[org.apache.spark.graphx.EdgeRDD#cache]] on the returned EdgeRDD.
   */
  override private[graphx] def withTargetStorageLevel(targetStorageLevel: StorageLevel) = {
    new GrapeEdgeRDDImpl[ED](this.grapePartitionsRDD, targetStorageLevel)
  }


  def mapEdgePartitions[ED2: ClassTag](
      f: (PartitionID, GrapeEdgePartition[ED]) => GrapeEdgePartition[ED2]): GrapeEdgeRDDImpl[ED2] = {
    this.withPartitionsRDD[ED2](grapePartitionsRDD.mapPartitions({ iter =>
      if (iter.hasNext) {
        val (pid, ep) = iter.next()
        Iterator(Tuple2(pid, f(pid, ep)))
      } else {
        Iterator.empty
      }
    }, preservesPartitioning = true))
  }

  private[graphx] def withPartitionsRDD[ED2: ClassTag](
       grapePartitionsRDD: RDD[(PartitionID, GrapeEdgePartition[ED2])]): GrapeEdgeRDDImpl[ED2] = {
    new GrapeEdgeRDDImpl[ED2](grapePartitionsRDD, this.targetStorageLevel)
  }

//  override def reverse: GrapeEdgeRDDImpl[ED] = mapEdgePartitions((pid, part) => part.reverse)

//  def filter(
//              epred: EdgeTriplet[VD, ED] => Boolean,
//              vpred: (VertexId, VD) => Boolean): EdgeRDDImpl[ED, VD] = {
//    mapEdgePartitions((pid, part) => part.filter(epred, vpred))
//  }

//  override def innerJoin[ED2: ClassTag, ED3: ClassTag](other: EdgeRDD[ED2])
//  (f: (VertexId, VertexId, ED, ED2) => ED3): GrapeEdgeRDDImpl[ED3] = {
//    val ed2Tag = classTag[ED2]
//    val ed3Tag = classTag[ED3]
//    this.withPartitionsRDD[ED3](grapePartitionsRDD.zipPartitions(other.grapePartitionsRDD, true) {
//      (thisIter, otherIter) =>
//        val (pid, thisEPart) = thisIter.next()
//        val (_, otherEPart) = otherIter.next()
//        Iterator(Tuple2(pid, thisEPart.innerJoin(otherEPart)(f)(ed2Tag, ed3Tag)))
//    })
//  }

//  def mapEdgePartitions[ED2: ClassTag, VD2: ClassTag](
//               f: (VertexId, (VertexId,VertexId,ED)) => EdgePartition[ED2, VD2]): EdgeRDDImpl[ED2, VD2] = {
//    this.withPartitionsRDD[ED2, VD2](grapePartitionsRDD.mapPartitions({ iter =>
//      if (iter.hasNext) {
//        val (pid, ep) = iter.next()
//        Iterator(Tuple2(pid, f(pid, ep)))
//      } else {
//        Iterator.empty
//      }
//    }, preservesPartitioning = true))
//  }

//  private[graphx] def withPartitionsRDD[ED2: ClassTag, VD2: ClassTag](
//                                                                       grapePartitionsRDD: RDD[(PartitionID, EdgePartition[ED2, VD2])]): EdgeRDDImpl[ED2, VD2] = {
//    new EdgeRDDImpl(grapePartitionsRDD, this.targetStorageLevel)
//  }

//  override private[graphx] def withTargetStorageLevel(
//                                                       targetStorageLevel: StorageLevel): EdgeRDDImpl[ED, VD] = {
//    new EdgeRDDImpl(this.grapePartitionsRDD, targetStorageLevel)
//  }

  /**
   * Just inherit but don't use.
   * @return
   */
  override private[graphx] def partitionsRDD = null

  def createVertexMapRDD(): RDD[(PartitionID, GrapeVertexMapPartition)] = {
    grapePartitionsRDD.mapPartitions(
      iter => {
        if (iter.hasNext){
          val tuple = iter.next()
          Iterator((tuple._1, tuple._2.getVertexMapPartition()))
        }
        else {
          Iterator.empty
        }
      }
    )
  }

  override def mapToFile(filePrefix: String, mappedSize : Long): Array[String] = {
    val registry = SharedMemoryRegistry.getOrCreate()
    grapePartitionsRDD.foreachPartition({
      iter => {
        if (iter.hasNext){
          val tuple = iter.next()
          val pid = tuple._1
          val partition = tuple._2
          val dstFile = filePrefix + pid
          val mappedBuffer = registry.mapFor(dstFile, mappedSize)
          log.info(s"Partition ${pid} got edge mm file ${dstFile} buffer ${mappedBuffer}")
          val startAddress = mappedBuffer.getAddr
          val bufferWriter = new MemoryMappedBufferWriter(startAddress, mappedSize)
          //Write data.
          val innerEdgeNum = partition.ivEdgeNum
          val totalBytes = 8L + 4L + 16 + innerEdgeNum * 16 + innerEdgeNum * bytesForType(edClass)
          log.info("Total bytes written: " + totalBytes)
          //First put header
          //| 8bytes    | 4Bytes   | 8bytes     | ......    | ......     |   8Bytes   | .....
          //| total-len | ed type  | srcOid len |  srcoids  |  dstOids   |   edata len |
          bufferWriter.writeLong(totalBytes)
          bufferWriter.writeInt(GrapeUtils.class2Int(edClass))
          bufferWriter.writeLong(16L * innerEdgeNum.toLong)

          var ind = 0
          while (ind < innerEdgeNum){
            bufferWriter.writeLong(partition.srcOid(ind))
          }
          ind = 0
          while (ind < innerEdgeNum){
            bufferWriter.writeLong(partition.dstOid(ind))
          }
          log.info(s"Partition: ${pid} Finish writing oid array of size ${innerEdgeNum} to ${dstFile}")

          bufferWriter.writeLong(innerEdgeNum.toLong * bytesForType[ED](edClass))

          ind = 0
          if (edClass.equals(classOf[Long])){
            while (ind < innerEdgeNum){
              bufferWriter.writeLong(partition.edgeData(ind).asInstanceOf[Long])
            }
          }
          else if (edClass.equals(classOf[Double])){
            while (ind < innerEdgeNum){
              bufferWriter.writeDouble(partition.edgeData(ind).asInstanceOf[Double])
            }
          }
          else if (edClass.equals(classOf[Int])){
            while (ind < innerEdgeNum){
              bufferWriter.writeInt(partition.edgeData(ind).asInstanceOf[Int])
            }
          }
          else {
            throw new IllegalStateException("Unsupported vd type: "+ edClass.getName)
          }
          log.info(s"Partition: ${pid} Finish writing vdata array of size ${innerEdgeNum} to ${dstFile}")
        }
      }
    })
    val mappedFileSet = grapePartitionsRDD.mapPartitions({
      iter => {
        Iterator(registry.getAllMappedFileNames(filePrefix))
      }
    })
    val mappedFileArray = mappedFileSet.collect()
    log.info("collect mappedFileSet: " + mappedFileSet.collect().mkString("Array(", ", ", ")"))
    mappedFileArray
  }
}
