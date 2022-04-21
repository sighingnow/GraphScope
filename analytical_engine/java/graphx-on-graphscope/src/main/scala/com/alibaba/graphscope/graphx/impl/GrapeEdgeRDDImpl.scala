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

package com.alibaba.graphscope.graphx.impl

import com.alibaba.graphscope.graphx.GrapeEdgeRDD
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, OneToOneDependency}

import scala.reflect.{ClassTag, classTag}

class GrapeEdgeRDDImpl[ED: ClassTag] private[graphx] (
          @transient override val partitionsRDD: RDD[(VertexId, Edge[ED])],
          val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  extends GrapeEdgeRDD[ED](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  override def setName(_name: String): this.type = {
    if (partitionsRDD.name != null) {
      partitionsRDD.setName(partitionsRDD.name + ", " + _name)
    } else {
      partitionsRDD.setName(_name)
    }
    this
  }
  setName("GrapeEdgeRDD")

  /**
   * If `partitionsRDD` already has a partitioner, use it. Otherwise assume that the
   * `PartitionID`s in `partitionsRDD` correspond to the actual partitions and create a new
   * partitioner that allows co-partitioning with `partitionsRDD`.
   */
  override val partitioner = {
    partitionsRDD.partitioner.orElse(Some(new HashPartitioner(partitions.length)))
  }
  log.info("Partitioner: " + partitioner)

  override def collect(): Array[Edge[ED]] = this.map(_.copy()).collect()

  /**
   * Persists the edge partitions at the specified storage level, ignoring any existing target
   * storage level.
   */
  override def persist(newLevel: StorageLevel): this.type = {
    partitionsRDD.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = false): this.type = {
    partitionsRDD.unpersist(blocking)
    this
  }

  /**
   * Persists the edge partitions using `targetStorageLevel`, which defaults to MEMORY_ONLY.
   */
  override def cache(): this.type = {
    partitionsRDD.persist(targetStorageLevel)
    this
  }

  override def getStorageLevel: StorageLevel = partitionsRDD.getStorageLevel

  override def checkpoint(): Unit = {
    partitionsRDD.checkpoint()
  }

  override def isCheckpointed: Boolean = {
    firstParent[(VertexId, (VertexId,VertexId,ED))].isCheckpointed
  }

  override def getCheckpointFile: Option[String] = {
    partitionsRDD.getCheckpointFile
  }

  /** The number of edges in the RDD. */
  override def count(): Long = {
//    partitionsRDD.map(_._2.size.toLong).fold(0)(_ + _)
    partitionsRDD.count()
  }

//  override def mapValues[ED2: ClassTag](f: Edge[ED] => ED2): G[ED2] =
//    mapEdgePartitions((pid, part) => part.map(f))

//  override def mapValues[ED2: ClassTag](f: Edge[ED] => ED2): GrapeEdgeRDDImpl[ED2, VD] = {
//    mapEdges(f);
////    mapEdgePartitions((pi
  // d, part) => part.map(f))
//  }

//  def mapEdges[ED2 : ClassTag](function: Edge[ED] => ED2) : GrapeEdgeRDDImpl[ED2] = {
//    partitionsRDD.mapPartitions({
//      iter => iter.
//    })
//    new GrapeEdgeRDDImpl[ED](partitionsRDD.map[ED2](tuple => (tuple._1, function.apply(tuple._2))))

  /**
   * Map the values in an edge partitioning preserving the structure but changing the values.
   *
   * @tparam ED2 the new edge value type
   * @param f the function from an edge to a new edge value
   * @return a new EdgeRDD containing the new edge values
   */
  override def mapValues[ED2: ClassTag](f: Edge[ED] => ED2): EdgeRDD[ED2] = {
    null
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
    null
  }

//  override def reverse: GrapeEdgeRDDImpl[ED, VD] = mapEdgePartitions((pid, part) => part.reverse)

//  def filter(
//              epred: EdgeTriplet[VD, ED] => Boolean,
//              vpred: (VertexId, VD) => Boolean): EdgeRDDImpl[ED, VD] = {
//    mapEdgePartitions((pid, part) => part.filter(epred, vpred))
//  }

//  override def innerJoin[ED2: ClassTag, ED3: ClassTag]
//  (other: EdgeRDD[ED2])
//  (f: (VertexId, VertexId, ED, ED2) => ED3): EdgeRDDImpl[ED3, VD] = {
//    val ed2Tag = classTag[ED2]
//    val ed3Tag = classTag[ED3]
//    this.withPartitionsRDD[ED3, VD](partitionsRDD.zipPartitions(other.partitionsRDD, true) {
//      (thisIter, otherIter) =>
//        val (pid, thisEPart) = thisIter.next()
//        val (_, otherEPart) = otherIter.next()
//        Iterator(Tuple2(pid, thisEPart.innerJoin(otherEPart)(f)(ed2Tag, ed3Tag)))
//    })
//  }

//  def mapEdgePartitions[ED2: ClassTag, VD2: ClassTag](
//               f: (VertexId, (VertexId,VertexId,ED)) => EdgePartition[ED2, VD2]): EdgeRDDImpl[ED2, VD2] = {
//    this.withPartitionsRDD[ED2, VD2](partitionsRDD.mapPartitions({ iter =>
//      if (iter.hasNext) {
//        val (pid, ep) = iter.next()
//        Iterator(Tuple2(pid, f(pid, ep)))
//      } else {
//        Iterator.empty
//      }
//    }, preservesPartitioning = true))
//  }

//  private[graphx] def withPartitionsRDD[ED2: ClassTag, VD2: ClassTag](
//                                                                       partitionsRDD: RDD[(PartitionID, EdgePartition[ED2, VD2])]): EdgeRDDImpl[ED2, VD2] = {
//    new EdgeRDDImpl(partitionsRDD, this.targetStorageLevel)
//  }

//  override private[graphx] def withTargetStorageLevel(
//                                                       targetStorageLevel: StorageLevel): EdgeRDDImpl[ED, VD] = {
//    new EdgeRDDImpl(this.partitionsRDD, targetStorageLevel)
//  }

}
