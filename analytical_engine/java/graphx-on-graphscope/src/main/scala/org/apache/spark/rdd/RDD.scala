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

package org.apache.spark.rdd

import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark._
import org.apache.spark.annotation.{DeveloperApi, Experimental, Since}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

import scala.collection.Map
import scala.io.Codec
import scala.language.implicitConversions
import scala.ref.WeakReference
import scala.reflect.{ClassTag, classTag}

/**
 * A Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable,
 * partitioned collection of elements that can be operated on in parallel. This class contains the
 * basic operations available on all RDDs, such as `map`, `filter`, and `persist`. In addition,
 * [[org.apache.spark.rdd.PairRDDFunctions]] contains operations available only on RDDs of key-value
 * pairs, such as `groupByKey` and `join`;
 * [[org.apache.spark.rdd.DoubleRDDFunctions]] contains operations available only on RDDs of
 * Doubles; and
 * [[org.apache.spark.rdd.SequenceFileRDDFunctions]] contains operations available on RDDs that
 * can be saved as SequenceFiles.
 * All operations are automatically available on any RDD of the right type (e.g. RDD[(Int, Int)])
 * through implicit.
 *
 * Internally, each RDD is characterized by five main properties:
 *
 *  - A list of partitions
 *  - A function for computing each split
 *  - A list of dependencies on other RDDs
 *  - Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned)
 *  - Optionally, a list of preferred locations to compute each split on (e.g. block locations for
 *    an HDFS file)
 *
 * All of the scheduling and execution in Spark is done based on these methods, allowing each RDD
 * to implement its own way of computing itself. Indeed, users can implement custom RDDs (e.g. for
 * reading data from a new storage system) by overriding these functions. Please refer to the
 * <a href="http://people.csail.mit.edu/matei/papers/2012/nsdi_spark.pdf">Spark paper</a>
 * for more details on RDD internals.
 */
abstract class RDD[T: ClassTag](
                                 @transient private var _sc: SparkContext,
                                 @transient private var deps: Seq[Dependency[_]]
                               ) extends Serializable with Logging {

  private[spark] def elementClassTag: ClassTag[T] = classTag[T]
  if (classOf[RDD[_]].isAssignableFrom(elementClassTag.runtimeClass)) {
    // This is a warning instead of an exception in order to avoid breaking user programs that
    // might have defined nested RDDs without running jobs with them.
    logWarning("Spark does not support nested RDDs (see SPARK-5063)")
  }

  private def sc: SparkContext = {
    if (_sc == null) {
      throw new SparkException(
        "This RDD lacks a SparkContext. It could happen in the following cases: \n(1) RDD " +
          "transformations and actions are NOT invoked by the driver, but inside of other " +
          "transformations; for example, rdd1.map(x => rdd2.values.count() * x) is invalid " +
          "because the values transformation and count action cannot be performed inside of the " +
          "rdd1.map transformation. For more information, see SPARK-5063.\n(2) When a Spark " +
          "Streaming job recovers from checkpoint, this exception will be hit if a reference to " +
          "an RDD not defined by the streaming job is used in DStream operations. For more " +
          "information, See SPARK-13758.")
    }
    _sc
  }

  /** Construct an RDD with just a one-to-one dependency on one parent */
  def this(@transient oneParent: RDD[_]) =
    this(oneParent.context, List(new OneToOneDependency(oneParent)))

//  private[spark] def conf = sc.conf
  // =======================================================================
  // Methods that should be implemented by subclasses of RDD
  // =======================================================================

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  @DeveloperApi
  def compute(split: Partition, context: TaskContext): Iterator[T]

  final def iterator(split: Partition, context: TaskContext): Iterator[T] = {
    throw new IllegalStateException()
  }

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   *
   * The partitions in this array must satisfy the following property:
   * `rdd.partitions.zipWithIndex.forall { case (partition, index) => partition.index == index }`
   */
  protected def getPartitions: Array[Partition]

  /**
   * Implemented by subclasses to return how this RDD depends on parent RDDs. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  protected def getDependencies: Seq[Dependency[_]] = deps

  /**
   * Optionally overridden by subclasses to specify placement preferences.
   */
  protected def getPreferredLocations(split: Partition): Seq[String] = Nil

  /** Optionally overridden by subclasses to specify how they are partitioned. */
  @transient val partitioner: Option[Partitioner] = None

  // =======================================================================
  // Methods and fields available on all RDDs
  // =======================================================================

  /** The SparkContext that created this RDD. */
  def sparkContext: SparkContext = sc

  /** A unique ID for this RDD (within its SparkContext). */
  val id: Int = sc.newRddId()

  /** A friendly name for this RDD */
  @transient var name: String = _

  /** Assign a name to this RDD */
  def setName(_name: String): this.type = {
    name = _name
    this
  }

  /**
   * Mark this RDD for persisting using the specified level.
   *
   * @param newLevel      the target storage level
   * @param allowOverride whether to override any existing level with the new one
   */
  private def persist(newLevel: StorageLevel, allowOverride: Boolean): this.type = {
    log.warn("Persist does nothing")
    this
  }

  /**
   * Set this RDD's storage level to persist its values across operations after the first time
   * it is computed. This can only be used to assign a new storage level if the RDD does not
   * have a storage level set yet. Local checkpointing is an exception.
   */
  def persist(newLevel: StorageLevel): this.type = {
    log.warn("Persist does nothing")
    this
  }

  /**
   * Persist this RDD with the default storage level (`MEMORY_ONLY`).
   */
  def persist(): this.type = persist(StorageLevel.MEMORY_ONLY)

  /**
   * Persist this RDD with the default storage level (`MEMORY_ONLY`).
   */
  def cache(): this.type = persist()

  /**
   * Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.
   *
   * @param blocking Whether to block until all blocks are deleted (default: false)
   * @return This RDD.
   */
  def unpersist(blocking: Boolean = false): this.type = {
    log.warn("UnPersist does nothing")
    this
  }

  /**
   * Lock for all mutable state of this RDD (persistence, partitions, dependencies, etc.).  We do
   * not use `this` because RDDs are user-visible, so users might have added their own locking on
   * RDDs; sharing that could lead to a deadlock.
   *
   * One thread might hold the lock on many of these, for a chain of RDD dependencies; but
   * because DAGs are acyclic, and we only ever hold locks for one path in that DAG, there is no
   * chance of deadlock.
   *
   * Executors may reference the shared fields (though they should never mutate them,
   * that only happens on the driver).
   */
  private val stateLock = new Serializable {}

  // Our dependencies and partitions will be gotten by calling subclass's methods below, and will
  // be overwritten when we're checkpointed
  @volatile private var dependencies_ : Seq[Dependency[_]] = _
  // When we overwrite the dependencies we keep a weak reference to the old dependencies
  // for user controlled cleanup.
  @volatile
  @transient private var legacyDependencies: WeakReference[Seq[Dependency[_]]] = _
  @volatile
  @transient private var partitions_ : Array[Partition] = _

  /**
   * Get the list of dependencies of this RDD, taking into account whether the
   * RDD is checkpointed or not.
   */
  final def dependencies: Seq[Dependency[_]] = {
    throw new IllegalStateException()
  }


  /**
   * Get the array of partitions of this RDD, taking into account whether the
   * RDD is checkpointed or not.
   */
  final def partitions: Array[Partition] = {
      partitions_
  }

  /**
   * Returns the number of partitions of this RDD.
   */
  @Since("1.6.0")
  final def getNumPartitions: Int = partitions.length


  // Transformations (return a new RDD)

  /**
   * Return a new RDD by applying a function to all elements of this RDD.
   */
  def map[U: ClassTag](f: T => U): RDD[U] = {
    throw new IllegalStateException();
  }

  /**
   * Return a new RDD by first applying a function to all elements of this
   * RDD, and then flattening the results.
   */
  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = {
    throw new IllegalStateException();
  }

  /**
   * Return a new RDD containing only the elements that satisfy a predicate.
   */
  def filter(f: T => Boolean): RDD[T] = {
    throw new IllegalStateException();
  }

  /**
   * Return a new RDD containing the distinct elements in this RDD.
   */
  def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = {
    throw new IllegalStateException();
  }

  /**
   * Return a new RDD containing the distinct elements in this RDD.
   */
  def distinct(): RDD[T] = {
    throw new IllegalStateException();
  }

  /**
   * Return a new RDD that has exactly numPartitions partitions.
   *
   * Can increase or decrease the level of parallelism in this RDD. Internally, this uses
   * a shuffle to redistribute data.
   *
   * If you are decreasing the number of partitions in this RDD, consider using `coalesce`,
   * which can avoid performing a shuffle.
   */
  def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = {
    throw new IllegalStateException();
  }

  /**
   * Return a new RDD that is reduced into `numPartitions` partitions.
   *
   * This results in a narrow dependency, e.g. if you go from 1000 partitions
   * to 100 partitions, there will not be a shuffle, instead each of the 100
   * new partitions will claim 10 of the current partitions. If a larger number
   * of partitions is requested, it will stay at the current number of partitions.
   *
   * However, if you're doing a drastic coalesce, e.g. to numPartitions = 1,
   * this may result in your computation taking place on fewer nodes than
   * you like (e.g. one node in the case of numPartitions = 1). To avoid this,
   * you can pass shuffle = true. This will add a shuffle step, but means the
   * current upstream partitions will be executed in parallel (per whatever
   * the current partitioning is).
   *
   * @note With shuffle = true, you can actually coalesce to a larger number
   *       of partitions. This is useful if you have a small number of partitions,
   *       say 100, potentially with a few partitions being abnormally large. Calling
   *       coalesce(1000, shuffle = true) will result in 1000 partitions with the
   *       data distributed using a hash partitioner. The optional partition coalescer
   *       passed in must be serializable.
   */
  def coalesce(numPartitions: Int, shuffle: Boolean = false,
               partitionCoalescer: Option[PartitionCoalescer] = Option.empty)
              (implicit ord: Ordering[T] = null)
  : RDD[T] = {
    throw new IllegalStateException();
  }

  /**
   * Return a sampled subset of this RDD.
   *
   * @param withReplacement can elements be sampled multiple times (replaced when sampled out)
   * @param fraction        expected size of the sample as a fraction of this RDD's size
   *                        without replacement: probability that each element is chosen; fraction must be [0, 1]
   *                        with replacement: expected number of times each element is chosen; fraction must be greater
   *                        than or equal to 0
   * @param seed            seed for the random number generator
   * @note This is NOT guaranteed to provide exactly the fraction of the count
   *       of the given [[RDD]].
   */
  def sample(
              withReplacement: Boolean,
              fraction: Double,
              seed: Long = Utils.random.nextLong): RDD[T] = {
    throw new IllegalStateException();
  }

  /**
   * Randomly splits this RDD with the provided weights.
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1
   * @param seed    random seed
   * @return split RDDs in an array
   */
  def randomSplit(
                   weights: Array[Double],
                   seed: Long = Utils.random.nextLong): Array[RDD[T]] = {
    throw new IllegalStateException();
  }


  /**
   * Return a fixed-size sampled subset of this RDD in an array
   *
   * @param withReplacement whether sampling is done with replacement
   * @param num             size of the returned sample
   * @param seed            seed for the random number generator
   * @return sample of specified size in an array
   * @note this method should only be used if the resulting array is expected to be small, as
   *       all the data is loaded into the driver's memory.
   */
  def takeSample(
                  withReplacement: Boolean,
                  num: Int,
                  seed: Long = Utils.random.nextLong): Array[T] = {
    throw new IllegalStateException();
  }

  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple
   * times (use `.distinct()` to eliminate them).
   */
  def union(other: RDD[T]): RDD[T] = {
    throw new IllegalStateException();
//    sc.union(this, other)
  }

  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple
   * times (use `.distinct()` to eliminate them).
   */
  def ++(other: RDD[T]): RDD[T] = {
    throw new IllegalStateException();
//    this.union(other)
  }

  /**
   * Return this RDD sorted by the given key function.
   */
  def sortBy[K](
                 f: (T) => K,
                 ascending: Boolean = true,
                 numPartitions: Int = this.partitions.length)
               (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T] = {
    throw new IllegalStateException();
  }

  /**
   * Return the intersection of this RDD and another one. The output will not contain any duplicate
   * elements, even if the input RDDs did.
   *
   * @note This method performs a shuffle internally.
   */
  def intersection(other: RDD[T]): RDD[T] = {
    throw new IllegalStateException();
  }

  /**
   * Return the intersection of this RDD and another one. The output will not contain any duplicate
   * elements, even if the input RDDs did.
   *
   * @note This method performs a shuffle internally.
   * @param partitioner Partitioner to use for the resulting RDD
   */
  def intersection(
                    other: RDD[T],
                    partitioner: Partitioner)(implicit ord: Ordering[T] = null): RDD[T] = {
    throw new IllegalStateException();
  }

  /**
   * Return the intersection of this RDD and another one. The output will not contain any duplicate
   * elements, even if the input RDDs did.  Performs a hash partition across the cluster
   *
   * @note This method performs a shuffle internally.
   * @param numPartitions How many partitions to use in the resulting RDD
   */
  def intersection(other: RDD[T], numPartitions: Int): RDD[T] = {
    intersection(other, new HashPartitioner(numPartitions))
  }

  /**
   * Return an RDD created by coalescing all elements within each partition into an array.
   */
  def glom(): RDD[Array[T]] = {
    throw new IllegalStateException();
  }

  /**
   * Return the Cartesian product of this RDD and another one, that is, the RDD of all pairs of
   * elements (a, b) where a is in `this` and b is in `other`.
   */
  def cartesian[U: ClassTag](other: RDD[U]): RDD[(T, U)] = {
    throw new IllegalStateException();
  }

  /**
   * Return an RDD of grouped items. Each group consists of a key and a sequence of elements
   * mapping to that key. The ordering of elements within each group is not guaranteed, and
   * may even differ each time the resulting RDD is evaluated.
   *
   * @note This operation may be very expensive. If you are grouping in order to perform an
   *       aggregation (such as a sum or average) over each key, using `PairRDDFunctions.aggregateByKey`
   *       or `PairRDDFunctions.reduceByKey` will provide much better performance.
   */
  def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = {
    throw new IllegalStateException();
  }

  /**
   * Return an RDD of grouped elements. Each group consists of a key and a sequence of elements
   * mapping to that key. The ordering of elements within each group is not guaranteed, and
   * may even differ each time the resulting RDD is evaluated.
   *
   * @note This operation may be very expensive. If you are grouping in order to perform an
   *       aggregation (such as a sum or average) over each key, using `PairRDDFunctions.aggregateByKey`
   *       or `PairRDDFunctions.reduceByKey` will provide much better performance.
   */
  def groupBy[K](
                  f: T => K,
                  numPartitions: Int)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = {
    throw new IllegalStateException();
  }

  /**
   * Return an RDD of grouped items. Each group consists of a key and a sequence of elements
   * mapping to that key. The ordering of elements within each group is not guaranteed, and
   * may even differ each time the resulting RDD is evaluated.
   *
   * @note This operation may be very expensive. If you are grouping in order to perform an
   *       aggregation (such as a sum or average) over each key, using `PairRDDFunctions.aggregateByKey`
   *       or `PairRDDFunctions.reduceByKey` will provide much better performance.
   */
  def groupBy[K](f: T => K, p: Partitioner)(implicit kt: ClassTag[K], ord: Ordering[K] = null)
  : RDD[(K, Iterable[T])] = {
    throw new IllegalStateException();
  }

  /**
   * Return an RDD created by piping elements to a forked external process.
   */
  def pipe(command: String): RDD[String] = {
    // Similar to Runtime.exec(), if we are given a single string, split it into words
    // using a standard StringTokenizer (i.e. by spaces)
    pipe(PipedRDD.tokenize(command))
  }

  /**
   * Return an RDD created by piping elements to a forked external process.
   */
  def pipe(command: String, env: Map[String, String]): RDD[String] = {
    // Similar to Runtime.exec(), if we are given a single string, split it into words
    // using a standard StringTokenizer (i.e. by spaces)
    pipe(PipedRDD.tokenize(command), env)
  }

  /**
   * Return an RDD created by piping elements to a forked external process. The resulting RDD
   * is computed by executing the given process once per partition. All elements
   * of each input partition are written to a process's stdin as lines of input separated
   * by a newline. The resulting partition consists of the process's stdout output, with
   * each line of stdout resulting in one element of the output partition. A process is invoked
   * even for empty partitions.
   *
   * The print behavior can be customized by providing two functions.
   *
   * @param command            command to run in forked process.
   * @param env                environment variables to set.
   * @param printPipeContext   Before piping elements, this function is called as an opportunity
   *                           to pipe context data. Print line function (like out.println) will be
   *                           passed as printPipeContext's parameter.
   * @param printRDDElement    Use this function to customize how to pipe elements. This function
   *                           will be called with each RDD element as the 1st parameter, and the
   *                           print line function (like out.println()) as the 2nd parameter.
   *                           An example of pipe the RDD data of groupBy() in a streaming way,
   *                           instead of constructing a huge String to concat all the elements:
   * {{{
   *                        def printRDDElement(record:(String, Seq[String]), f:String=>Unit) =
   *                          for (e <- record._2) {f(e)}
   * }}}
   * @param separateWorkingDir Use separate working directories for each task.
   * @param bufferSize         Buffer size for the stdin writer for the piped process.
   * @param encoding           Char encoding used for interacting (via stdin, stdout and stderr) with
   *                           the piped process
   * @return the result RDD
   */
  def pipe(
            command: Seq[String],
            env: Map[String, String] = Map(),
            printPipeContext: (String => Unit) => Unit = null,
            printRDDElement: (T, String => Unit) => Unit = null,
            separateWorkingDir: Boolean = false,
            bufferSize: Int = 8192,
            encoding: String = Codec.defaultCharsetCodec.name): RDD[String] = {
    throw new IllegalStateException();
  }

  /**
   * Return a new RDD by applying a function to each partition of this RDD.
   *
   * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
   * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
   */
  def mapPartitions[U: ClassTag](
                                  f: Iterator[T] => Iterator[U],
                                  preservesPartitioning: Boolean = false): RDD[U] = {
    throw new IllegalStateException();
  }


  /**
   * Return a new RDD by applying a function to each partition of this RDD, while tracking the index
   * of the original partition.
   *
   * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
   * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
   */
  def mapPartitionsWithIndex[U: ClassTag](
                                           f: (Int, Iterator[T]) => Iterator[U],
                                           preservesPartitioning: Boolean = false): RDD[U] = {
    throw new IllegalStateException();
  }

  /**
   * Zips this RDD with another one, returning key-value pairs with the first element in each RDD,
   * second element in each RDD, etc. Assumes that the two RDDs have the *same number of
   * partitions* and the *same number of elements in each partition* (e.g. one was made through
   * a map on the other).
   */
  def zip[U: ClassTag](other: RDD[U]): RDD[(T, U)] = {
    throw new IllegalStateException();
  }

  /**
   * Zip this RDD's partitions with one (or more) RDD(s) and return a new RDD by
   * applying a function to the zipped partitions. Assumes that all the RDDs have the
   * *same number of partitions*, but does *not* require them to have the same number
   * of elements in each partition.
   */
  def zipPartitions[B: ClassTag, V: ClassTag]
  (rdd2: RDD[B], preservesPartitioning: Boolean)
  (f: (Iterator[T], Iterator[B]) => Iterator[V]): RDD[V] = {
    throw new IllegalStateException();
  }

  def getStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY


  // Actions (launch a job to return a value to the user program)

  /**
   * Applies a function f to all elements of this RDD.
   */
  def foreach(f: T => Unit): Unit = {
    throw new IllegalStateException();
  }

  /**
   * Applies a function f to each partition of this RDD.
   */
  def foreachPartition(f: Iterator[T] => Unit): Unit = {
    throw new IllegalStateException();
  }

  /**
   * Return an array that contains all of the elements in this RDD.
   *
   * @note This method should only be used if the resulting array is expected to be small, as
   *       all the data is loaded into the driver's memory.
   */
  def collect(): Array[T] = {
    throw new IllegalStateException();
  }

  /**
   * Return an iterator that contains all of the elements in this RDD.
   *
   * The iterator will consume as much memory as the largest partition in this RDD.
   *
   * @note This results in multiple Spark jobs, and if the input RDD is the result
   *       of a wide transformation (e.g. join with different partitioners), to avoid
   *       recomputing the input RDD should be cached first.
   */
  def toLocalIterator: Iterator[T] = {
    throw new IllegalStateException();
  }

  /**
   * Return an RDD that contains all matching values by applying `f`.
   */
  def collect[U: ClassTag](f: PartialFunction[T, U]): RDD[U] = {
    throw new IllegalStateException();
  }

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   *
   * Uses `this` partitioner/partition size, because even if `other` is huge, the resulting
   * RDD will be &lt;= us.
   */
  def subtract(other: RDD[T]): RDD[T] = {
    throw new IllegalStateException();
  }

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   */
  def subtract(other: RDD[T], numPartitions: Int): RDD[T] = {
    throw new IllegalStateException();
  }

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
   */
  def subtract(
                other: RDD[T],
                p: Partitioner)(implicit ord: Ordering[T] = null): RDD[T] = {
    throw new IllegalStateException();
  }

  /**
   * Reduces the elements of this RDD using the specified commutative and
   * associative binary operator.
   */
  def reduce(f: (T, T) => T): T = {
    throw new IllegalStateException();
  }

  /**
   * Reduces the elements of this RDD in a multi-level tree pattern.
   *
   * @param depth suggested depth of the tree (default: 2)
   * @see [[org.apache.spark.rdd.RDD#reduce]]
   */
  def treeReduce(f: (T, T) => T, depth: Int = 2): T = {
    throw new IllegalStateException();
  }

  /**
   * Aggregate the elements of each partition, and then the results for all the partitions, using a
   * given associative function and a neutral "zero value". The function
   * op(t1, t2) is allowed to modify t1 and return it as its result value to avoid object
   * allocation; however, it should not modify t2.
   *
   * This behaves somewhat differently from fold operations implemented for non-distributed
   * collections in functional languages like Scala. This fold operation may be applied to
   * partitions individually, and then fold those results into the final result, rather than
   * apply the fold to each element sequentially in some defined ordering. For functions
   * that are not commutative, the result may differ from that of a fold applied to a
   * non-distributed collection.
   *
   * @param zeroValue the initial value for the accumulated result of each partition for the `op`
   *                  operator, and also the initial value for the combine results from different
   *                  partitions for the `op` operator - this will typically be the neutral
   *                  element (e.g. `Nil` for list concatenation or `0` for summation)
   * @param op        an operator used to both accumulate results within a partition and combine results
   *                  from different partitions
   */
  def fold(zeroValue: T)(op: (T, T) => T): T = {
    throw new IllegalStateException();
  }

  /**
   * Aggregate the elements of each partition, and then the results for all the partitions, using
   * given combine functions and a neutral "zero value". This function can return a different result
   * type, U, than the type of this RDD, T. Thus, we need one operation for merging a T into an U
   * and one operation for merging two U's, as in scala.TraversableOnce. Both of these functions are
   * allowed to modify and return their first argument instead of creating a new U to avoid memory
   * allocation.
   *
   * @param zeroValue the initial value for the accumulated result of each partition for the
   *                  `seqOp` operator, and also the initial value for the combine results from
   *                  different partitions for the `combOp` operator - this will typically be the
   *                  neutral element (e.g. `Nil` for list concatenation or `0` for summation)
   * @param seqOp     an operator used to accumulate results within a partition
   * @param combOp    an associative operator used to combine results from different partitions
   */
  def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U = {
    throw new IllegalStateException();
  }

  /**
   * Aggregates the elements of this RDD in a multi-level tree pattern.
   * This method is semantically identical to [[org.apache.spark.rdd.RDD#aggregate]].
   *
   * @param depth suggested depth of the tree (default: 2)
   */
  def treeAggregate[U: ClassTag](zeroValue: U)(
    seqOp: (U, T) => U,
    combOp: (U, U) => U,
    depth: Int = 2): U = {
    throw new IllegalStateException();
  }

  /**
   * Return the number of elements in the RDD.
   */
  def count(): Long = 0

  /**
   * Approximate version of count() that returns a potentially incomplete result
   * within a timeout, even if not all tasks have finished.
   *
   * The confidence is the probability that the error bounds of the result will
   * contain the true value. That is, if countApprox were called repeatedly
   * with confidence 0.9, we would expect 90% of the results to contain the
   * true count. The confidence must be in the range [0,1] or an exception will
   * be thrown.
   *
   * @param timeout    maximum time to wait for the job, in milliseconds
   * @param confidence the desired statistical confidence in the result
   * @return a potentially incomplete result, with error bounds
   */
  def countApprox(
                   timeout: Long,
                   confidence: Double = 0.95): PartialResult[BoundedDouble] = {
    throw new IllegalStateException();
  }

  /**
   * Return the count of each unique value in this RDD as a local map of (value, count) pairs.
   *
   * @note This method should only be used if the resulting map is expected to be small, as
   *       the whole thing is loaded into the driver's memory.
   *       To handle very large results, consider using
   *
   * {{{
   * rdd.map(x => (x, 1L)).reduceByKey(_ + _)
   * }}}
   *
   *       , which returns an RDD[T, Long] instead of a map.
   */
  def countByValue()(implicit ord: Ordering[T] = null): Map[T, Long] = {
    map(value => (value, null)).countByKey()
  }

  /**
   * Approximate version of countByValue().
   *
   * @param timeout    maximum time to wait for the job, in milliseconds
   * @param confidence the desired statistical confidence in the result
   * @return a potentially incomplete result, with error bounds
   */
  def countByValueApprox(timeout: Long, confidence: Double = 0.95)
                        (implicit ord: Ordering[T] = null)
  : PartialResult[Map[T, BoundedDouble]] = {
    throw new IllegalStateException();
  }

  /**
   * Return approximate number of distinct elements in the RDD.
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="https://doi.org/10.1145/2452376.2452456">here</a>.
   *
   * The relative accuracy is approximately `1.054 / sqrt(2^p)`. Setting a nonzero (`sp` is greater
   * than `p`) would trigger sparse representation of registers, which may reduce the memory
   * consumption and increase accuracy when the cardinality is small.
   *
   * @param p  The precision value for the normal set.
   *           `p` must be a value between 4 and `sp` if `sp` is not zero (32 max).
   * @param sp The precision value for the sparse set, between 0 and 32.
   *           If `sp` equals 0, the sparse representation is skipped.
   */
  def countApproxDistinct(p: Int, sp: Int): Long = {
    throw new IllegalStateException();
  }

  /**
   * Return approximate number of distinct elements in the RDD.
   *
   * The algorithm used is based on streamlib's implementation of "HyperLogLog in Practice:
   * Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm", available
   * <a href="https://doi.org/10.1145/2452376.2452456">here</a>.
   *
   * @param relativeSD Relative accuracy. Smaller values create counters that require more space.
   *                   It must be greater than 0.000017.
   */
  def countApproxDistinct(relativeSD: Double = 0.05): Long = {
    throw new IllegalStateException();
  }

  /**
   * Zips this RDD with its element indices. The ordering is first based on the partition index
   * and then the ordering of items within each partition. So the first item in the first
   * partition gets index 0, and the last item in the last partition receives the largest index.
   *
   * This is similar to Scala's zipWithIndex but it uses Long instead of Int as the index type.
   * This method needs to trigger a spark job when this RDD contains more than one partitions.
   *
   * @note Some RDDs, such as those returned by groupBy(), do not guarantee order of
   *       elements in a partition. The index assigned to each element is therefore not guaranteed,
   *       and may even change if the RDD is reevaluated. If a fixed ordering is required to guarantee
   *       the same index assignments, you should sort the RDD with sortByKey() or save it to a file.
   */
  def zipWithIndex(): RDD[(T, Long)] = {
    throw new IllegalStateException();
  }

  /**
   * Zips this RDD with generated unique Long ids. Items in the kth partition will get ids k, n+k,
   * 2*n+k, ..., where n is the number of partitions. So there may exist gaps, but this method
   * won't trigger a spark job, which is different from [[org.apache.spark.rdd.RDD#zipWithIndex]].
   *
   * @note Some RDDs, such as those returned by groupBy(), do not guarantee order of
   *       elements in a partition. The unique ID assigned to each element is therefore not guaranteed,
   *       and may even change if the RDD is reevaluated. If a fixed ordering is required to guarantee
   *       the same index assignments, you should sort the RDD with sortByKey() or save it to a file.
   */
  def zipWithUniqueId(): RDD[(T, Long)] = {
    throw new IllegalStateException();
  }

  /**
   * Take the first num elements of the RDD. It works by first scanning one partition, and use the
   * results from that partition to estimate the number of additional partitions needed to satisfy
   * the limit.
   *
   * @note This method should only be used if the resulting array is expected to be small, as
   *       all the data is loaded into the driver's memory.
   * @note Due to complications in the internal implementation, this method will raise
   *       an exception if called on an RDD of `Nothing` or `Null`.
   */
  def take(num: Int): Array[T] = {
    throw new IllegalStateException();
  }

  /**
   * Return the first element in this RDD.
   */
  def first(): T = {
    throw new IllegalStateException();
  }

  /**
   * Returns the top k (largest) elements from this RDD as defined by the specified
   * implicit Ordering[T] and maintains the ordering. This does the opposite of
   * [[takeOrdered]]. For example:
   * {{{
   *   sc.parallelize(Seq(10, 4, 2, 12, 3)).top(1)
   *   // returns Array(12)
   *
   *   sc.parallelize(Seq(2, 3, 4, 5, 6)).top(2)
   *   // returns Array(6, 5)
   * }}}
   *
   * @note This method should only be used if the resulting array is expected to be small, as
   *       all the data is loaded into the driver's memory.
   * @param num k, the number of top elements to return
   * @param ord the implicit ordering for T
   * @return an array of top elements
   */
  def top(num: Int)(implicit ord: Ordering[T]): Array[T] = {
    throw new IllegalStateException();
  }

  /**
   * Returns the first k (smallest) elements from this RDD as defined by the specified
   * implicit Ordering[T] and maintains the ordering. This does the opposite of [[top]].
   * For example:
   * {{{
   *   sc.parallelize(Seq(10, 4, 2, 12, 3)).takeOrdered(1)
   *   // returns Array(2)
   *
   *   sc.parallelize(Seq(2, 3, 4, 5, 6)).takeOrdered(2)
   *   // returns Array(2, 3)
   * }}}
   *
   * @note This method should only be used if the resulting array is expected to be small, as
   *       all the data is loaded into the driver's memory.
   * @param num k, the number of elements to return
   * @param ord the implicit ordering for T
   * @return an array of top elements
   */
  def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T] = {
    throw new IllegalStateException();
  }

  /**
   * Returns the max of this RDD as defined by the implicit Ordering[T].
   *
   * @return the maximum element of the RDD
   * */
  def max()(implicit ord: Ordering[T]): T = {
    throw new IllegalStateException();
  }

  /**
   * Returns the min of this RDD as defined by the implicit Ordering[T].
   *
   * @return the minimum element of the RDD
   * */
  def min()(implicit ord: Ordering[T]): T = {
    throw new IllegalStateException();
  }

  /**
   * @note Due to complications in the internal implementation, this method will raise an
   *       exception if called on an RDD of `Nothing` or `Null`. This may be come up in practice
   *       because, for example, the type of `parallelize(Seq())` is `RDD[Nothing]`.
   *       (`parallelize(Seq())` should be avoided anyway in favor of `parallelize(Seq[T]())`.)
   * @return true if and only if the RDD contains no elements at all. Note that an RDD
   *         may be empty even when it has at least 1 partition.
   */
  def isEmpty(): Boolean = {
    throw new IllegalStateException();
  }

  /**
   * Save this RDD as a text file, using string representations of elements.
   */
  def saveAsTextFile(path: String): Unit = {
    throw new IllegalStateException();
  }

  /**
   * Save this RDD as a compressed text file, using string representations of elements.
   */
  def saveAsTextFile(path: String, codec: Class[_ <: CompressionCodec]): Unit = {
    throw new IllegalStateException();
  }

  /**
   * Save this RDD as a SequenceFile of serialized objects.
   */
  def saveAsObjectFile(path: String): Unit = {
    throw new IllegalStateException();
  }

  /**
   * Creates tuples of the elements in this RDD by applying `f`.
   */
  def keyBy[K](f: T => K): RDD[(K, T)] = {
    throw new IllegalStateException();
  }


  /**
   * Mark this RDD for checkpointing. It will be saved to a file inside the checkpoint
   * directory set with `SparkContext#setCheckpointDir` and all references to its parent
   * RDDs will be removed. This function must be called before any job has been
   * executed on this RDD. It is strongly recommended that this RDD is persisted in
   * memory, otherwise saving it on a file will require recomputation.
   */
  def checkpoint(): Unit = RDDCheckpointData.synchronized {
    throw new IllegalStateException();
  }

  /**
   * Mark this RDD for local checkpointing using Spark's existing caching layer.
   *
   * This method is for users who wish to truncate RDD lineages while skipping the expensive
   * step of replicating the materialized data in a reliable distributed file system. This is
   * useful for RDDs with long lineages that need to be truncated periodically (e.g. GraphX).
   *
   * Local checkpointing sacrifices fault-tolerance for performance. In particular, checkpointed
   * data is written to ephemeral local storage in the executors instead of to a reliable,
   * fault-tolerant storage. The effect is that if an executor fails during the computation,
   * the checkpointed data may no longer be accessible, causing an irrecoverable job failure.
   *
   * This is NOT safe to use with dynamic allocation, which removes executors along
   * with their cached blocks. If you must use both features, you are advised to set
   * `spark.dynamicAllocation.cachedExecutorIdleTimeout` to a high value.
   *
   * The checkpoint directory set through `SparkContext#setCheckpointDir` is not used.
   */
  def localCheckpoint(): this.type = RDDCheckpointData.synchronized {
    throw new IllegalStateException();
  }

  /**
   * Return whether this RDD is checkpointed and materialized, either reliably or locally.
   */
  def isCheckpointed: Boolean = false

  /**
   * Gets the name of the directory to which this RDD was checkpointed.
   * This is not defined if the RDD is checkpointed locally.
   */
  def getCheckpointFile: Option[String] = {
    throw new IllegalStateException();
  }

  /**
   * :: Experimental ::
   * Removes an RDD's shuffles and it's non-persisted ancestors.
   * When running without a shuffle service, cleaning up shuffle files enables downscaling.
   * If you use the RDD after this call, you should checkpoint and materialize it first.
   * If you are uncertain of what you are doing, please do not use this feature.
   * Additional techniques for mitigating orphaned shuffle files:
   * * Tuning the driver GC to be more aggressive, so the regular context cleaner is triggered
   * * Setting an appropriate TTL for shuffle files to be auto cleaned
   */
  @Experimental
  @DeveloperApi
  @Since("3.1.0")
  def cleanShuffleDependencies(blocking: Boolean = false): Unit = {
    throw new IllegalStateException();
  }


  /**
   * :: Experimental ::
   * Marks the current stage as a barrier stage, where Spark must launch all tasks together.
   * In case of a task failure, instead of only restarting the failed task, Spark will abort the
   * entire stage and re-launch all tasks for this stage.
   * The barrier execution mode feature is experimental and it only handles limited scenarios.
   * Please read the linked SPIP and design docs to understand the limitations and future plans.
   *
   * @return an [[RDDBarrier]] instance that provides actions within a barrier stage
   * @see [[org.apache.spark.BarrierTaskContext]]
   * @see <a href="https://jira.apache.org/jira/browse/SPARK-24374">SPIP: Barrier Execution Mode</a>
   * @see <a href="https://jira.apache.org/jira/browse/SPARK-24582">Design Doc</a>
   */
  @Experimental
  @Since("2.4.0")
  def barrier(): RDDBarrier[T] = (new RDDBarrier[T](this))

  /**
   * Specify a ResourceProfile to use when calculating this RDD. This is only supported on
   * certain cluster managers and currently requires dynamic allocation to be enabled.
   * It will result in new executors with the resources specified being acquired to
   * calculate the RDD.
   */
  @Experimental
  @Since("3.1.0")
  def withResources(rp: ResourceProfile): this.type = {
    throw new IllegalStateException();
  }

  @transient private var resourceProfile: Option[ResourceProfile] = None
  /**
   * Get the ResourceProfile specified with this RDD or null if it wasn't specified.
   *
   * @return the user specified ResourceProfile or null (for Java compatibility) if
   *         none was specified
   */
  @Experimental
  @Since("3.1.0")
  def getResourceProfile(): ResourceProfile = resourceProfile.getOrElse(null)

  // =======================================================================
  // Other internal methods and fields
  // =======================================================================


  /** Returns the first parent RDD */
  protected[spark] def firstParent[U: ClassTag]: RDD[U] = {
    dependencies.head.rdd.asInstanceOf[RDD[U]]
  }

  /** Returns the jth parent RDD: e.g. rdd.parent[T](0) is equivalent to rdd.firstParent[T] */
  protected[spark] def parent[U: ClassTag](j: Int): RDD[U] = {
    dependencies(j).rdd.asInstanceOf[RDD[U]]
  }

  /** The [[org.apache.spark.SparkContext]] that this RDD was created on. */
  def context: SparkContext = sc


  /**
   * Clears the dependencies of this RDD. This method must ensure that all references
   * to the original parent RDDs are removed to enable the parent RDDs to be garbage
   * collected. Subclasses of RDD may override this method for implementing their own cleaning
   * logic. See [[org.apache.spark.rdd.UnionRDD]] for an example.
   */
  protected def clearDependencies(): Unit = stateLock.synchronized {
    dependencies_ = null
  }

  /** A description of this RDD and its recursive dependencies for debugging. */
  def toDebugString: String = {
    "Null"
  }

  override def toString: String = "%s%s[%d]".format(
    Option(name).map(_ + " ").getOrElse(""), getClass.getSimpleName, id)

  def toJavaRDD(): JavaRDD[T] = {
    throw new IllegalStateException();
  }

  /**
   * Whether the RDD is in a barrier stage. Spark must launch all the tasks at the same time for a
   * barrier stage.
   *
   * An RDD is in a barrier stage, if at least one of its parent RDD(s), or itself, are mapped from
   * an [[RDDBarrier]]. This function always returns false for a [[ShuffledRDD]], since a
   * [[ShuffledRDD]] indicates start of a new stage.
   *
   * A [[MapPartitionsRDD]] can be transformed from an [[RDDBarrier]], under that case the
   * [[MapPartitionsRDD]] shall be marked as barrier.
   */
  private[spark] def isBarrier(): Boolean = isBarrier_

  // From performance concern, cache the value to avoid repeatedly compute `isBarrier()` on a long
  // RDD chain.
  @transient protected lazy val isBarrier_ : Boolean =
  dependencies.filter(!_.isInstanceOf[ShuffleDependency[_, _, _]]).exists(_.rdd.isBarrier())

}


/**
 * Defines implicit functions that provide extra functionalities on RDDs of specific types.
 *
 * For example, [[RDD.rddToPairRDDFunctions]] converts an RDD into a [[PairRDDFunctions]] for
 * key-value-pair RDDs, and enabling extra functionalities such as `PairRDDFunctions.reduceByKey`.
 */
object RDD {

  private[spark] val CHECKPOINT_ALL_MARKED_ANCESTORS =
    "spark.checkpoint.checkpointAllMarkedAncestors"

  // The following implicit functions were in SparkContext before 1.3 and users had to
  // `import SparkContext._` to enable them. Now we move them here to make the compiler find
  // them automatically. However, we still keep the old functions in SparkContext for backward
  // compatibility and forward to the following functions directly.

  implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
                                          (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
    new PairRDDFunctions(rdd)
  }

  implicit def rddToAsyncRDDActions[T: ClassTag](rdd: RDD[T]): AsyncRDDActions[T] = {
    new AsyncRDDActions(rdd)
  }

  implicit def rddToSequenceFileRDDFunctions[K, V](rdd: RDD[(K, V)])
                                                  (implicit kt: ClassTag[K], vt: ClassTag[V],
                                                   keyWritableFactory: WritableFactory[K],
                                                   valueWritableFactory: WritableFactory[V])
  : SequenceFileRDDFunctions[K, V] = {
    implicit val keyConverter = keyWritableFactory.convert
    implicit val valueConverter = valueWritableFactory.convert
    new SequenceFileRDDFunctions(rdd,
      keyWritableFactory.writableClass(kt), valueWritableFactory.writableClass(vt))
  }

  implicit def rddToOrderedRDDFunctions[K: Ordering : ClassTag, V: ClassTag](rdd: RDD[(K, V)])
  : OrderedRDDFunctions[K, V, (K, V)] = {
    new OrderedRDDFunctions[K, V, (K, V)](rdd)
  }

  implicit def doubleRDDToDoubleRDDFunctions(rdd: RDD[Double]): DoubleRDDFunctions = {
    new DoubleRDDFunctions(rdd)
  }

  implicit def numericRDDToDoubleRDDFunctions[T](rdd: RDD[T])(implicit num: Numeric[T])
  : DoubleRDDFunctions = {
    new DoubleRDDFunctions(rdd.map(x => num.toDouble(x)))
  }
}

/**
 * The deterministic level of RDD's output (i.e. what `RDD#compute` returns). This explains how
 * the output will diff when Spark reruns the tasks for the RDD. There are 3 deterministic levels:
 * 1. DETERMINATE: The RDD output is always the same data set in the same order after a rerun.
 * 2. UNORDERED: The RDD output is always the same data set but the order can be different
 * after a rerun.
 * 3. INDETERMINATE. The RDD output can be different after a rerun.
 *
 * Note that, the output of an RDD usually relies on the parent RDDs. When the parent RDD's output
 * is INDETERMINATE, it's very likely the RDD's output is also INDETERMINATE.
 */
private[spark] object DeterministicLevel extends Enumeration {
  val DETERMINATE, UNORDERED, INDETERMINATE = Value
}
