package org.apache.spark.graphx.impl.grape

import org.apache.spark.graphx.impl.{EdgePartition, EdgeRDDImpl, GrapeEdgePartition, ShippableVertexPartition}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, OneToOneDependency, Partition, Partitioner, TaskContext}

import scala.reflect.ClassTag

/**
 * This class should be construct from off heap memories, which is provided by graphscope.
 * @param partitionsRDD
 * @param targetStorageLevel
 * @param vdTag
 * @tparam VD
 */
class GrapeVertexRDDImpl[VD] private[graphx](
                                                 @transient val grapePartitionsRDD: RDD[(PartitionID, GrapeVertexPartition[VD])],
                                                 val targetStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
                                            (implicit override protected val vdTag: ClassTag[VD])
  extends GrapeVertexRDD[VD](grapePartitionsRDD.context, List(new OneToOneDependency(grapePartitionsRDD))) {

  override def reindex(): VertexRDD[VD] = {
    throw new IllegalStateException("Not implemented")
  }
  override protected def getPartitions: Array[Partition] = grapePartitionsRDD.partitions

  override val partitioner: Option[Partitioner] = grapePartitionsRDD.partitioner

  override def compute(part: Partition, context: TaskContext): Iterator[(VertexId,VD)] = {
    val p = firstParent[(PartitionID, GrapeVertexPartition[VD])].iterator(part, context)
    if (p.hasNext) {
      //      p.next()._2.iterator.map(_.copy())
      p.next()._2.iterator.map(_.copy())
    } else {
      Iterator.empty
    }
  }

  override def count(): Long = {
    grapePartitionsRDD.map(_._2.partitionVnum.toLong).fold(0)(_ + _)
  }

  override private[graphx] def mapVertexPartitions[VD2](f: ShippableVertexPartition[VD] => ShippableVertexPartition[VD2])(implicit evidence$1: ClassTag[VD2]) = {
    throw new IllegalStateException("Not implemented")
  }
  override private[graphx] def mapGrapeVertexPartitions[VD2: ClassTag](f: GrapeVertexPartition[VD] => GrapeVertexPartition[VD2])
  : GrapeVertexRDD[VD2] = {
    val newPartitionsRDD  = grapePartitionsRDD.mapPartitions(
      iter => {
        if (iter.hasNext){
          val tuple = iter.next();
          val pid = tuple._1
          val partition = tuple._2
          Iterator((pid, f.apply(partition)))
        }
        else {
          Iterator.empty
        }
      },preservesPartitioning = true
    )
    this.withGrapePartitionsRDD(newPartitionsRDD)
  }

  override def mapValues[VD2](f: VD => VD2)(implicit evidence$2: ClassTag[VD2]): VertexRDD[VD2] = {
    this.mapGrapeVertexPartitions(_.map((vid, attr) => f(attr)))
  }

  override def mapValues[VD2](f: (VertexId, VD) => VD2)(implicit evidence$3: ClassTag[VD2]): VertexRDD[VD2] = {
    this.mapGrapeVertexPartitions(_.map(f))
  }

  override def minus(other: RDD[(VertexId, VD)]): VertexRDD[VD] = {
    minus(this.aggregateUsingIndex(other, (a: VD, b: VD) => a))
  }

  override def minus(other: VertexRDD[VD]): VertexRDD[VD] = {
    other match{
      case other : GrapeVertexRDDImpl[VD] if this.partitioner == other.partitioner => {
        this.withGrapePartitionsRDD[VD](
          grapePartitionsRDD.zipPartitions(
            other.grapePartitionsRDD, preservesPartitioning = true) {
            (thisIter, otherIter) =>
              val thisTuple = thisIter.next()
              val thisPartition = thisTuple._2
              val otherTuple = otherIter.next()
              require(thisTuple._1 == otherTuple._1, "partition id should match")
              Iterator((thisTuple._1, thisPartition.minus(otherTuple._2)))
          })
      }
      case _ =>  throw new IllegalArgumentException("can only minus a grape vertex rdd now")
    }
  }

  override def diff(other: RDD[(VertexId, VD)]): VertexRDD[VD] = {
    diff(this.aggregateUsingIndex(other, (a: VD, b: VD) => a))
  }

  override def diff(other: VertexRDD[VD]): VertexRDD[VD] = {
    val otherPartition = other match {
      case other: GrapeVertexRDDImpl[VD] if this.partitioner == other.partitioner =>
        other.grapePartitionsRDD
      case _ =>  throw new IllegalArgumentException("can only minus a grape vertex rdd now")
    }
    val newPartitionsRDD = grapePartitionsRDD.zipPartitions(
      otherPartition, preservesPartitioning = true
    ) { (thisIter, otherIter) =>
      val thisTuple = thisIter.next()
      val otherTuple = otherIter.next()
      require(thisTuple._1 == otherTuple._1, "partition id should match")
      Iterator((thisTuple._1, thisTuple._2.diff(otherTuple._2)))
    }
    this.withGrapePartitionsRDD(newPartitionsRDD)
  }

  override def leftZipJoin[VD2 : ClassTag, VD3 : ClassTag](other: VertexRDD[VD2])(f: (VertexId, VD, Option[VD2]) => VD3): VertexRDD[VD3] = {
    val newPartitionsRDD = other match {
      case other : GrapeVertexRDDImpl[VD2] => {
        grapePartitionsRDD.zipPartitions(
          other.grapePartitionsRDD, preservesPartitioning = true
        ) { (thisIter, otherIter) =>
          val thisTuple = thisIter.next()
          val otherTuple = otherIter.next()
          Iterator((thisTuple._1, thisTuple._2.leftJoin(otherTuple._2)(f)))
        }
      }
    }
    this.withGrapePartitionsRDD(newPartitionsRDD)
  }

  override def leftJoin[VD2, VD3](other: RDD[(VertexId, VD2)])(f: (VertexId, VD, Option[VD2]) => VD3)(implicit evidence$6: ClassTag[VD2], evidence$7: ClassTag[VD3]): VertexRDD[VD3] = {
    other match {
      case other: GrapeVertexRDDImpl[_] if this.partitioner == other.partitioner =>
        leftZipJoin(other)(f)
      case _ =>
        throw new IllegalArgumentException("currently not support to join with non-grape vertex rdd")
    }
  }

  override def innerZipJoin[U : ClassTag, VD2 : ClassTag](other: VertexRDD[U])(f: (VertexId, VD, U) => VD2): VertexRDD[VD2] = {
    val newPartitionsRDD = other match {
      case other : GrapeVertexRDDImpl[U] => {
        grapePartitionsRDD.zipPartitions(
          other.grapePartitionsRDD, preservesPartitioning = true
        ) { (thisIter, otherIter) =>
          val thisTuple = thisIter.next()
          val otherTuple = otherIter.next()
          Iterator((thisTuple._1, thisTuple._2.innerJoin(otherTuple._2)(f)))
        }
      }
    }
    this.withGrapePartitionsRDD(newPartitionsRDD)
  }

  override def innerJoin[U, VD2](other: RDD[(VertexId, U)])(f: (VertexId, VD, U) => VD2)(implicit evidence$10: ClassTag[U], evidence$11: ClassTag[VD2]): VertexRDD[VD2] = {
    other match {
      case other: GrapeVertexRDDImpl[U] if this.partitioner == other.partitioner =>
        innerZipJoin(other)(f)
      case _ =>
          throw new IllegalArgumentException("Current only support join with grape vertex rdd");
    }
  }

  /**
   * To aggregate with us, the input rdd can be a offheap vertexRDDImpl or a common one.
   * - For offHeap rdd ,just do the aggregation, since its partitioner is same with use, i.e. hashPartition
   *   with fnum = num of workers.
   * - For common one, we need to make sure they share the same num of partitions. and then we repartition to size of excutors.
   * @param messages
   * @param reduceFunc
   * @tparam VD2
   * @return
   */
  override def aggregateUsingIndex[VD2: ClassTag](messages: RDD[(VertexId, VD2)], reduceFunc: (VD2, VD2) => VD2)
                                       : VertexRDD[VD2] = {
    val executorStatus = sparkContext.getExecutorMemoryStatus
    log.info(s"Current available executors ${executorStatus}")
    val numExecutors = executorStatus.size
    val shuffled = messages.partitionBy(new HashPartitioner(numExecutors))
    val newPartitionsRDD = grapePartitionsRDD.zipPartitions(shuffled,true) {
      (thisIter, msgIter) =>{
        if (thisIter.hasNext){
          val tuple = thisIter.next();
          val newPartition = tuple._2.aggregateUsingIndex(msgIter, reduceFunc)
          Iterator((tuple._1, newPartition))
        }
        else {
          Iterator.empty
        }
      }
    }
    this.withGrapePartitionsRDD[VD2](newPartitionsRDD)
  }

  override def writeBackVertexData(vdataMappedPath : String, vdataMappedSize : Long): Unit = {
    grapePartitionsRDD.foreachPartition(iter => {
      if (iter.hasNext){
        val tuple = iter.next()
        val pid = tuple._1
        val part = tuple._2
        FragmentRegistry.updateVertexPartition(pid, part)
      }
    })
    grapePartitionsRDD.foreachPartition(iter => {
      if (iter.hasNext){
        val tuple = iter.next()
        val pid = tuple._1
        val part = tuple._2
        FragmentRegistry.mapVertexData(pid, vdataMappedPath, vdataMappedSize)
      }
    })
  }

  override def reverseRoutingTables(): VertexRDD[VD] = {
    throw new IllegalStateException("Inherited but not implemented, should not be used")
  }

  override def withEdges(edges: EdgeRDD[_]): VertexRDD[VD] = {
    throw new IllegalStateException("Inherited but not implemented, should not be used")
  }

  override private[graphx] def withPartitionsRDD[VD2](partitionsRDD: RDD[ShippableVertexPartition[VD2]])(implicit evidence$13: ClassTag[VD2]) = {
    throw new IllegalStateException("Inherited but not implemented, should not be used, use withGrapePartitionsRDD instead")
  }

  override private[graphx] def withGrapePartitionsRDD[VD2 : ClassTag](partitionsRDD: RDD[(PartitionID, GrapeVertexPartition[VD2])]) : GrapeVertexRDD[VD2] = {
    new GrapeVertexRDDImpl[VD2](partitionsRDD,this.targetStorageLevel)
  }

  override private[graphx] def withTargetStorageLevel(newTargetStorageLevel: StorageLevel) = {
    new GrapeVertexRDDImpl[VD](grapePartitionsRDD, newTargetStorageLevel)
  }

  override private[graphx] def shipVertexAttributes(shipSrc: Boolean, shipDst: Boolean) = {
    throw new IllegalStateException("Inherited but not implemented, should not be used")
  }

  override private[graphx] def shipVertexIds() = {
    throw new IllegalStateException("Inherited but not implemented, should not be used")
    }

  override private[graphx] def partitionsRDD = {
    throw new IllegalStateException("Inherited but not implemented, should not be used")
  }
}
