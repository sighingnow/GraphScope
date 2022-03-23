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

import com.alibaba.graphscope.communication.Communicator
import com.alibaba.graphscope.graph.IdManager

import scala.reflect.ClassTag
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

/**
 * Our substitute class for VertexRDDImpl, change the underline engine to grape.
 * @param partitionsRDD
 * @param targetStorageLevel
 * @param vdTag
 * @tparam VD
 */
class VertexRDDImpl[VD] private[graphx] (sc : SparkContext, innerVerticesNum : VertexId,
                                         private val idManager : IdManager,
                                         private val vdata : Array[VD])
                                        (implicit override protected val vdTag: ClassTag[VD])
  extends VertexRDD[VD](sc, Nil) {

  @transient val partitionsRDD: RDD[ShippableVertexPartition[VD]] = null
  //Although this logger is not static, but it should still work well
  private val logger = LoggerFactory.getLogger(classOf[VertexRDDImpl[VD]].getName)


  override def reindex(): VertexRDD[VD] = {
    throw new IllegalStateException("No implementation for reindex")
  }

//  override val partitioner =

//  override protected def getPreferredLocations(s: Partition): Seq[String] =
//    partitionsRDD.preferredLocations(s)

//  override def setName(_name: String): this.type = {
//    if (partitionsRDD.name != null) {
//      partitionsRDD.setName(partitionsRDD.name + ", " + _name)
//    } else {
//      partitionsRDD.setName(_name)
//    }
//    this
//  }
  setName("VertexRDD")

  /**
   * Persists the vertex partitions at the specified storage level, ignoring any existing target
   * storage level.
   */
  override def persist(newLevel: StorageLevel): this.type = {
    logger.info("Calling persist on VertexRDDImpl is meaningless.")
    this
  }

  override def unpersist(blocking: Boolean = false): this.type = {
    logger.info("Calling persist on VertexRDDImpl is meaningless.")
    this
  }

  /**
   * Persists the vertex partitions at `targetStorageLevel`, which defaults to MEMORY_ONLY.
   */
  override def cache(): this.type = {
    logger.info("Calling cache on VertexRDDImpl is meaningless.")
    this
  }

  override def getStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY

  override def checkpoint(): Unit = {
    logger.info("Calling checkpoint on VertexRDDImpl is meaningless.")
  }

  override def isCheckpointed: Boolean = {
    logger.info("Calling IsCheckPointed on VertexRDDImpl is meaningless.")
    true
  }

  override def getCheckpointFile: Option[String] = {
    Some("Null")
  }

  /** The number of vertices in the RDD. */
  override def count(): Long = {
    innerVerticesNum
  }

  override def mapValues[VD2: ClassTag](f: VD => VD2): VertexRDD[VD2] ={
    new VertexRDDImpl[VD2](sc, innerVerticesNum, idManager, vdata.map(f))
  }

  override def mapValues[VD2: ClassTag](f: (VertexId, VD) => VD2): VertexRDD[VD2] = {
    val oidArray : Array[Long] = idManager.getOidArray
    new VertexRDDImpl[VD2](sc, innerVerticesNum, idManager, oidArray.zip(vdata).map{ case (vid, vd ) => f(vid, vd)})
  }

  override def minus(other: RDD[(VertexId, VD)]): VertexRDD[VD] = {
    logger.warn("Not implemented")
    this
  }

  override def minus (other: VertexRDD[VD]): VertexRDD[VD] = {
    logger.warn("Not implemented")
    this
  }

  override def diff(other: RDD[(VertexId, VD)]): VertexRDD[VD] = {
    diff(this.aggregateUsingIndex(other, (a: VD, b: VD) => a))
  }

  override def diff(other: VertexRDD[VD]): VertexRDD[VD] = {
    logger.info("Not implemented")
    this
  }

//  override def take(num: Int): VertexRDD[VD] = {
//
//  }

  override private[graphx] def mapVertexPartitions[VD2](f: ShippableVertexPartition[VD] => ShippableVertexPartition[VD2])(implicit evidence$1: ClassTag[VD2]) = ???

  override def leftZipJoin[VD2, VD3](other: VertexRDD[VD2])(f: (VertexId, VD, Option[VD2]) => VD3)(implicit evidence$4: ClassTag[VD2], evidence$5: ClassTag[VD3]): VertexRDD[VD3] = ???

  override def leftJoin[VD2, VD3](other: RDD[(VertexId, VD2)])(f: (VertexId, VD, Option[VD2]) => VD3)(implicit evidence$6: ClassTag[VD2], evidence$7: ClassTag[VD3]): VertexRDD[VD3] = ???

  override def innerZipJoin[U, VD2](other: VertexRDD[U])(f: (VertexId, VD, U) => VD2)(implicit evidence$8: ClassTag[U], evidence$9: ClassTag[VD2]): VertexRDD[VD2] = ???

  override def innerJoin[U, VD2](other: RDD[(VertexId, U)])(f: (VertexId, VD, U) => VD2)(implicit evidence$10: ClassTag[U], evidence$11: ClassTag[VD2]): VertexRDD[VD2] = ???

  override def aggregateUsingIndex[VD2](messages: RDD[(VertexId, VD2)], reduceFunc: (VD2, VD2) => VD2)(implicit evidence$12: ClassTag[VD2]): VertexRDD[VD2] = ???

  override def reverseRoutingTables(): VertexRDD[VD] = ???

  override def withEdges(edges: EdgeRDD[_]): VertexRDD[VD] = ???

  override private[graphx] def withPartitionsRDD[VD2](partitionsRDD: RDD[ShippableVertexPartition[VD2]])(implicit evidence$13: ClassTag[VD2]) = ???

  override private[graphx] def withTargetStorageLevel(targetStorageLevel: StorageLevel) = ???

  override private[graphx] def shipVertexAttributes(shipSrc: Boolean, shipDst: Boolean) = ???

  override private[graphx] def shipVertexIds() = ???
}
