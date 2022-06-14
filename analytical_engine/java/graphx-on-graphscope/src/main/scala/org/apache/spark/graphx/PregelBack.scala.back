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

package org.apache.spark.graphx

import com.alibaba.graphscope.communication.Communicator
import com.alibaba.graphscope.graphx.SerializationUtils
import com.alibaba.graphscope.parallel.DefaultMessageManager
import com.alibaba.graphscope.utils.{CallUtils, MPIProcessLauncher, MappedBuffer}
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkContext, TaskContext, graphx}

import java.io.{BufferedWriter, File, FileWriter}
import java.util.concurrent.atomic.AtomicInteger
import scala.reflect.{ClassTag, classTag}

/**
 * Implements a Pregel-like bulk-synchronous message-passing API.
 *
 * Unlike the original Pregel API, the GraphX Pregel API factors the sendMessage computation over
 * edges, enables the message sending computation to read both vertex attributes, and constrains
 * messages to the graph structure.  These changes allow for substantially more efficient
 * distributed execution while also exposing greater flexibility for graph-based computation.
 *
 * @example We can use the Pregel abstraction to implement PageRank:
 * {{{
 * val pagerankGraph: Graph[Double, Double] = graph
 *   // Associate the degree with each vertex
 *   .outerJoinVertices(graph.outDegrees) {
 *     (vid, vdata, deg) => deg.getOrElse(0)
 *   }
 *   // Set the weight on the edges based on the degree
 *   .mapTriplets(e => 1.0 / e.srcAttr)
 *   // Set the vertex attributes to the initial pagerank values
 *   .mapVertices((id, attr) => 1.0)
 *
 * def vertexProgram(id: VertexId, attr: Double, msgSum: Double): Double =
 *   resetProb + (1.0 - resetProb) * msgSum
 * def sendMessage(id: VertexId, edge: EdgeTriplet[Double, Double]): Iterator[(VertexId, Double)] =
 *   Iterator((edge.dstId, edge.srcAttr * edge.attr))
 * def messageCombiner(a: Double, b: Double): Double = a + b
 * val initialMessage = 0.0
 * // Execute Pregel for a fixed number of iterations.
 * Pregel(pagerankGraph, initialMessage, numIter)(
 *   vertexProgram, sendMessage, messageCombiner)
 * }}}
 *
 *          User use this entry class to provide three essential function object, and we shall use them to do
 *          the query and message sending. The GraphXAdaptor will drive the computation
 *
 */
object PregelBack extends Logging {
  val MAPPED_SIZE : Long  = 2L * 1024 * 1024 * 1024; //8 GB.
  var comm: Communicator = null
  var messageManager: DefaultMessageManager = null
  val MMAP_FILE_PREFIX = "/graphx-"
  val MMAP_V_FILE_PREFIX = MMAP_FILE_PREFIX + "vertex-"
  val MMAP_E_FILE_PREFIX = MMAP_FILE_PREFIX + "edge-"
  val V_FILE_LOG_PREFIX = "/tmp/graphx-vertex-log-"
  val E_FILE_LOG_PREFIX = "/tmp/graphx-edge-log-"
  val VPROG_SERIALIZATION_PATH = "/tmp/graphx-vprog"
  val SEND_MSG_SERIALIZATION_PATH = "/tmp/graphx-sendMsg"
  val MERGE_MSG_SERIALIZATION_PATH = "/tmp/graphx-mergeMsg"

  def myClassOf[T: ClassTag] = implicitly[ClassTag[T]].runtimeClass

  def setCommunicator(communicator: Communicator) = comm = communicator

  def setMessageManager(msger: DefaultMessageManager) = messageManager = msger

  /**
   * Execute a Pregel-like iterative vertex-parallel abstraction.  The
   * user-defined vertex-program `vprog` is executed in parallel on
   * each vertex receiving any inbound messages and computing a new
   * value for the vertex.  The `sendMsg` function is then invoked on
   * all out-edges and is used to compute an optional message to the
   * destination vertex. The `mergeMsg` function is a commutative
   * associative function used to combine messages destined to the
   * same vertex.
   *
   * On the first iteration all vertices receive the `initialMsg` and
   * on subsequent iterations if a vertex does not receive a message
   * then the vertex-program is not invoked.
   *
   * This function iterates until there are no remaining messages, or
   * for `maxIterations` iterations.
   *
   * @tparam VD the vertex data type
   * @tparam ED the edge data type
   * @tparam A  the Pregel message type
   * @param graph           the input graph.
   * @param initialMsg      the message each vertex will receive at the first
   *                        iteration
   * @param maxIterations   the maximum number of iterations to run for
   * @param activeDirection the direction of edges incident to a vertex that received a message in
   *                        the previous round on which to run `sendMsg`. For example, if this is `EdgeDirection.Out`, only
   *                        out-edges of vertices that received a message in the previous round will run. The default is
   *                        `EdgeDirection.Either`, which will run `sendMsg` on edges where either side received a message
   *                        in the previous round. If this is `EdgeDirection.Both`, `sendMsg` will only run on edges where
   *                        *both* vertices received a message.
   * @param vprog           the user-defined vertex program which runs on each
   *                        vertex and receives the inbound message and computes a new vertex
   *                        value.  On the first iteration the vertex program is invoked on
   *                        all vertices and is passed the default message.  On subsequent
   *                        iterations the vertex program is only invoked on those vertices
   *                        that receive messages.
   * @param sendMsg         a user supplied function that is applied to out
   *                        edges of vertices that received messages in the current
   *                        iteration
   * @param mergeMsg        a user supplied function that takes two incoming
   *                        messages of type A and merges them into a single message of type
   *                        A. ''This function must be commutative and associative and
                                * ideally the size of A should not increase.''
   * @return the resulting graph at the end of the computation
   *
   */
  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]
  (graph: Graph[VD, ED],
   initialMsg: A,
   maxIterations: Int = Int.MaxValue,
   activeDirection: EdgeDirection = EdgeDirection.Either)
  (vprog: (VertexId, VD, A) => VD,
   sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
   mergeMsg: (A, A) => A)
  : Graph[VD, ED] = {
    require(maxIterations > 0, s"Maximum number of iterations must be greater than 0," +
      s" but got ${maxIterations}")
    //    require(messageManager != null, s"messager null")
    //    require(comm != null, s"comm null")
    log.info("Pregel method invoked")
    val vdClass = classTag[VD].runtimeClass.asInstanceOf[java.lang.Class[VD]]
    val edClass = classTag[ED].runtimeClass.asInstanceOf[java.lang.Class[ED]]
    val msgClass = classTag[A].runtimeClass.asInstanceOf[java.lang.Class[A]]
    log.info(s"vd class: ${vdClass} ed : ${edClass} msg ${msgClass}")

    val startTime = System.nanoTime();
    log.info("vertex partitions rdd number: " + graph.vertices.partitionsRDD.count())
    log.info("edge partitions rdd num: " + graph.edges.partitionsRDD.count())
    graph.vertices.foreachPartition(
      iterator => {
        val pid = TaskContext.getPartitionId()
        val loggerFileName = V_FILE_LOG_PREFIX + pid
        val bufferedWriter = new BufferedWriter(new FileWriter(new File(loggerFileName)))
        val strName = s"${MMAP_V_FILE_PREFIX}${pid}"
        val buffer = MappedBuffer.mapToFile(strName, MAPPED_SIZE);
        if (buffer == null){
          bufferedWriter.write("Error: mapped faild")
        }
        bufferedWriter.write(buffer.toString);
        bufferedWriter.newLine();
        bufferedWriter.write(s"for iterator in ${pid}")
        buffer.position(8) // reserve place to write total length
        //To put vd and ed in the header.
        putHeader(buffer,vdClass, edClass, msgClass, bufferedWriter)
        bufferedWriter.write("successfully put header + \n")
        val t1 = System.nanoTime()
        putVertices(buffer, iterator, vdClass, bufferedWriter)
        bufferedWriter.write("successfully put data limit, " + buffer.limit() + ", total length: " + buffer.position() + ", data size:" + (buffer.position() - 8));
        val t2 = System.nanoTime()
        bufferedWriter.write(" time for writing vertices " + (t2 - t1) / 1000000)
        bufferedWriter.newLine()
        buffer.writeLong(0, buffer.position() - 8)
        //      iterator
        bufferedWriter.close()
      }
    )
    val verticesTime = System.nanoTime()
    log.info(" vertices partition {}, partitions rdd partitions {}",graph.vertices.getNumPartitions, graph.vertices.partitionsRDD.getNumPartitions)
    log.info(" time spend on write vertices: " + (verticesTime - startTime) / 1000000)
    log.info(" edges partition {}, partitions rdd partitions {}",graph.edges.getNumPartitions, graph.edges.partitionsRDD.getNumPartitions)

    graph.edges.foreachPartition(
      iterator =>{
        val tt0 = System.nanoTime()
        val pid = TaskContext.getPartitionId()
        val loggerFileName = E_FILE_LOG_PREFIX + pid
        val bufferedWriter = new BufferedWriter(new FileWriter(new File(loggerFileName)))
        val strName = s"${MMAP_E_FILE_PREFIX}${pid}"
        val buffer = MappedBuffer.mapToFile(strName, MAPPED_SIZE)
        bufferedWriter.newLine()
        if (buffer == null){
          bufferedWriter.write("Error: mapped faild")
        }
        buffer.position(8) // reserve place to write total length
        //To put vd and ed in the header.
        //      putHeader(buffer, classTag[VD].runtimeClass.asInstanceOf[java.lang.Class[VD]], classTag[ED].runtimeClass.asInstanceOf[java.lang.Class[ED]], classTag[A].runtimeClass.asInstanceOf[java.lang.Class[A]]);
        //      bufferedWriter.write("successfully put header + \n")
        val t1 = System.nanoTime()
        putEdges(buffer, iterator, edClass, bufferedWriter)
        bufferedWriter.write("successfully put data limit, " + buffer.limit() + ", total length: " + buffer.position() + ", data size:" + (buffer.position() - 8))
        val t2 = System.nanoTime()
        bufferedWriter.write("time for write edges " + (t2 - t1) / 1000000)
        bufferedWriter.newLine()
        buffer.writeLong(0, buffer.position() - 8)
        //      iterator
        val endTime = System.nanoTime();
        bufferedWriter.write("total time spend on converting edges : " + (endTime - tt0) / 1000000 + "ms")
        bufferedWriter.newLine()
        bufferedWriter.close()
      }
    )
    val edgeTime = System.nanoTime()

    //graph.edges.mapPartitionsWithIndex

    //    val vprogRes = graph.vertices.mapPartitionsWithIndex((index, iterator) => {
    //
    //      iterator
    //    })
    //serialize vprog to driver, and send file to nodes via scp(by mpi launcher).
    SerializationUtils.write(vprog, VPROG_SERIALIZATION_PATH)
    SerializationUtils.write(sendMsg, SEND_MSG_SERIALIZATION_PATH)
    SerializationUtils.write(mergeMsg, MERGE_MSG_SERIALIZATION_PATH)
    val functionsTime = System.nanoTime()
    //    verticesRes.count() //force running
    //    edgesRes.count()
    //    vprogRes.count()
    val endTime = System.nanoTime();
    log.info(s"Time spend on memory mapping and serialization: " + (endTime - startTime) / 1000000 + " ms")
    log.info("Time spend on edges " + (edgeTime - verticesTime) / 1000000 + "ms")
    log.info("Time spend on serializing functions " + (functionsTime - edgeTime) / 1000000 + "ms")

    //    log.info(s"after writing to memory mapped file, launch mpi processes ${verticesRes}, ${edgesRes}")

    var userClass = CallUtils.getCallerCallerClassName
    if (userClass.endsWith("$")) {
      userClass = userClass.substring(0, userClass.length - 1)
    }
    log.info(s"call site ${userClass}")
    val mpiLauncher = new MPIProcessLauncher(MMAP_V_FILE_PREFIX, MMAP_E_FILE_PREFIX,
      VPROG_SERIALIZATION_PATH, SEND_MSG_SERIALIZATION_PATH, MERGE_MSG_SERIALIZATION_PATH, userClass, vdClass,edClass, msgClass, initialMsg, maxIterations, Math.max(graph.vertices.getNumPartitions, graph.edges.getNumPartitions), MAPPED_SIZE)
    mpiLauncher.run()

    graph
  } // end of apply

  def putHeader[VD: ClassTag, ED: ClassTag, A: ClassTag](buffer: MappedBuffer, vdClass: Class[VD], edClass: Class[ED], msgClass: Class[A], writer: BufferedWriter): Unit = {
    require(buffer.remaining() > 4, s"not enough space in buffer")
    writer.write("put vd class int " + class2Int(vdClass))
    writer.newLine()
    buffer.writeInt(class2Int(vdClass))
    require(buffer.remaining() > 4, s"not enough space in buffer")
    writer.write("put ed class int " + class2Int(edClass))
    writer.newLine()
    buffer.writeInt(class2Int(edClass))
    require(buffer.remaining() > 4, s"not enough space in buffer")
    writer.write("put msg class int " + class2Int(msgClass))
    writer.newLine()
    buffer.writeInt(class2Int(msgClass))
  }

  def putVertices[VD: ClassTag](buffer: MappedBuffer, array: Array[(graphx.VertexId, VD)], vdClass: Class[VD], writer: BufferedWriter): Unit = {
    /**
     * FIXME: tune this position cost, copy memory at once.
     * FIXME: construct arrow array from pointer.
     */
    if (vdClass.equals(classOf[java.lang.Long])) {
      for (i <- 0 until array.length){
        val vid = array(i)._1
        val vdata = array(i)._2
        //        require(buffer.position() > 0 && buffer.position() < buffer.limit(), "buffer position error")
        buffer.writeLong(vid)
        buffer.writeLong(vdata.asInstanceOf[java.lang.Long])
      }
    }
    else if (vdClass.equals(classOf[Long])) {
      for (i <- 0 until array.length){
        val vid = array(i)._1
        val vdata = array(i)._2
        //        require(buffer.position() > 0 && buffer.position() < buffer.limit(), "buffer position error")
        buffer.writeLong(vid)
        buffer.writeLong(vdata.asInstanceOf[Long])
      }
    }
    else if (vdClass.equals(classOf[java.lang.Double])) {
      for (i <- 0 until array.length){
        val vid = array(i)._1
        val vdata = array(i)._2
        //        require(buffer.position() > 0 && buffer.position() < buffer.limit(), "buffer position error")
        buffer.writeLong(vid)
        buffer.writeDouble(vdata.asInstanceOf[java.lang.Double])
      }
    }
    else if (vdClass.equals(classOf[Double])) {
      for (i <- 0 until array.length){
        val vid = array(i)._1
        val vdata = array(i)._2
        //        require(buffer.position() > 0 && buffer.position() < buffer.limit(), "buffer position error")
        buffer.writeLong(vid)
        buffer.writeDouble(vdata.asInstanceOf[Double])
      }
    }
    else if (vdClass.equals(classOf[java.lang.Integer])) {
      for (i <- 0 until array.length){
        val vid = array(i)._1
        val vdata = array(i)._2
        //        require(buffer.position() > 0 && buffer.position() < buffer.limit(), "buffer position error")
        buffer.writeLong(vid)
        buffer.writeInt(vdata.asInstanceOf[java.lang.Integer])
      }
    }
    else if (vdClass.equals(classOf[Int])) {
      for (i <- 0 until array.length){
        val vid = array(i)._1
        val vdata = array(i)._2
        //        require(buffer.position() > 0 && buffer.position() < buffer.limit(), "buffer position error")
        buffer.writeLong(vid)
        buffer.writeInt(vdata.asInstanceOf[Int])
      }
    }
    else throw new IllegalStateException("unexpected vdata class " + vdClass.getName)
  }

  def putVertices[VD: ClassTag](buffer: MappedBuffer, iter: Iterator[(graphx.VertexId, VD)], vdClass: Class[VD], writer: BufferedWriter): Unit = {
    /**
     * FIXME: tune this position cost, copy memory at once.
     * FIXME: construct arrow array from pointer.
     */
    if (vdClass.equals(classOf[java.lang.Long])) {
      //      buffer.writeLong(firstId)
      //      buffer.writeLong(firstVd.asInstanceOf[java.lang.Long])
      //      writer.write("put first vertex [" + firstId + "] -> [" + firstVd + " ] \n")
      while (iter.hasNext){
        val tuple = iter.next()
        buffer.writeLong(tuple._1)
        buffer.writeLong(tuple._2.asInstanceOf[java.lang.Long])
      }
    }
    else if (vdClass.equals(classOf[Long])) {
      //      buffer.writeLong(firstId)
      //      buffer.writeLong(firstVd.asInstanceOf[Long])
      //      writer.write("put first vertex [" + firstId + "] -> [" + firstVd + " ] \n")
      while (iter.hasNext){
        val tuple = iter.next()
        buffer.writeLong(tuple._1)
        buffer.writeLong(tuple._2.asInstanceOf[Long])
        //          writer.write("put first vertex [" + tuple._1 + "] -> [" + tuple._2 + " ] \n")
      }
    }
    else if (vdClass.equals(classOf[java.lang.Double])) {
      //      buffer.writeLong(firstId)
      //      buffer.writeDouble(firstVd.asInstanceOf[java.lang.Double])
      //      writer.write("put first vertex [" + firstId + "] -> [" + firstVd + " ] \n")
      while (iter.hasNext){
        val tuple = iter.next()
        buffer.writeLong(tuple._1)
        buffer.writeDouble(tuple._2.asInstanceOf[java.lang.Double])
        //          writer.write("put first vertex [" + tuple._1 + "] -> [" + tuple._2 + " ] \n")
      }
    }
    else if (vdClass.equals(classOf[Double])) {
      //      buffer.writeLong(firstId)
      //      buffer.writeDouble(firstVd.asInstanceOf[Double])
      //      writer.write("put first vertex [" + firstId + "] -> [" + firstVd + " ] \n")
      while (iter.hasNext){
        val tuple = iter.next()
        buffer.writeLong(tuple._1)
        buffer.writeDouble(tuple._2.asInstanceOf[Double])
        //          writer.write("put first vertex [" + tuple._1 + "] -> [" + tuple._2 + " ] \n")
      }
    }
    else if (vdClass.equals(classOf[java.lang.Integer])) {
      //      buffer.writeLong(firstId)
      //      buffer.writeInt(firstVd.asInstanceOf[java.lang.Integer])
      //      writer.write("put first vertex [" + firstId + "] -> [" + firstVd + " ] \n")
      while (iter.hasNext){
        val tuple = iter.next()
        buffer.writeLong(tuple._1)
        buffer.writeInt(tuple._2.asInstanceOf[java.lang.Integer])
        //          writer.write("put first vertex [" + tuple._1 + "] -> [" + tuple._2 + " ] \n")
      }
    }
    else if (vdClass.equals(classOf[Int])) {
      //      buffer.writeLong(firstId)
      //      buffer.writeInt(firstVd.asInstanceOf[java.lang.Integer])
      //      writer.write("put first vertex [" + firstId + "] -> [" + firstVd + " ] \n")
      while (iter.hasNext){
        val tuple = iter.next()
        buffer.writeLong(tuple._1)
        buffer.writeLong(tuple._2.asInstanceOf[Int])
        //          writer.write("put first vertex [" + tuple._1 + "] -> [" + tuple._2 + " ] \n")
      }
    }
    else throw new IllegalStateException("unexpected vdata class " + vdClass.getName)
  }
  def putEdges[ED: ClassTag](buffer: MappedBuffer, array: Array[Edge[ED]], edClass: Class[ED], writer: BufferedWriter): Unit = {
    if (edClass.equals(classOf[java.lang.Long])) {
      for (i <- 0 until array.length){
        val srcId = array(i).srcId
        val dstId = array(i).dstId
        val edgeAttr = array(i).attr
        //        require(buffer.position() > 0 && buffer.position() < buffer.limit(), "buffer position error")
        buffer.writeLong(srcId)
        buffer.writeLong(dstId)
        buffer.writeLong(edgeAttr.asInstanceOf[java.lang.Long])
      }
    }
    else if (edClass.equals(classOf[Long])) {
      for (i <- 0 until array.length){
        val srcId = array(i).srcId
        val dstId = array(i).dstId
        val edgeAttr = array(i).attr
        //        require(buffer.position() > 0 && buffer.position() < buffer.limit(), "buffer position error")
        buffer.writeLong(srcId)
        buffer.writeLong(dstId)
        buffer.writeLong(edgeAttr.asInstanceOf[Long])
      }
    }
    else if (edClass.equals(classOf[java.lang.Double])) {
      for (i <- 0 until array.length){
        val srcId = array(i).srcId
        val dstId = array(i).dstId
        val edgeAttr = array(i).attr
        //        require(buffer.position() > 0 && buffer.position() < buffer.limit(), "buffer position error")
        buffer.writeLong(srcId)
        buffer.writeLong(dstId)
        buffer.writeDouble(edgeAttr.asInstanceOf[java.lang.Double])
      }
    }
    else if (edClass.equals(classOf[Double])) {
      for (i <- 0 until array.length){
        val srcId = array(i).srcId
        val dstId = array(i).dstId
        val edgeAttr = array(i).attr
        //        require(buffer.position() > 0 && buffer.position() < buffer.limit(), "buffer position error")
        buffer.writeLong(srcId)
        buffer.writeLong(dstId)
        buffer.writeDouble(edgeAttr.asInstanceOf[Double])
      }
    }
    else if (edClass.equals(classOf[java.lang.Integer])) {
      for (i <- 0 until array.length){
        val srcId = array(i).srcId
        val dstId = array(i).dstId
        val edgeAttr = array(i).attr
        //        require(buffer.position() > 0 && buffer.position() < buffer.limit(), "buffer position error")
        buffer.writeLong(srcId)
        buffer.writeLong(dstId)
        buffer.writeInt(edgeAttr.asInstanceOf[java.lang.Integer])
      }
    }
    else if (edClass.equals(classOf[Int])) {
      for (i <- 0 until array.length){
        val srcId = array(i).srcId
        val dstId = array(i).dstId
        val edgeAttr = array(i).attr
        //        require(buffer.position() > 0 && buffer.position() < buffer.limit(), "buffer position error")
        buffer.writeLong(srcId)
        buffer.writeLong(dstId)
        buffer.writeInt(edgeAttr.asInstanceOf[Int])
      }
    }
    else throw new IllegalStateException("Unexpected ed class " + edClass.getName)
  }
  def putEdges[ED: ClassTag](buffer: MappedBuffer, iter: Iterator[Edge[ED]], edClass: Class[ED], writer: BufferedWriter): Unit = {
    if (edClass.equals(classOf[java.lang.Long])) {
      //      buffer.writeLong(firstSrcId)
      //      buffer.writeLong(firstDstid)
      //      buffer.writeLong(firstAttr.asInstanceOf[java.lang.Long])
      //      writer.write("put first edge [" + firstSrcId + "] -> [" + firstDstid + " ] , " + firstAttr + "\n")
      while (iter.hasNext){
        val tuple = iter.next()
        buffer.writeLong(tuple.srcId)
        buffer.writeLong(tuple.dstId)
        buffer.writeLong(tuple.attr.asInstanceOf[java.lang.Long])
        //          writer.write("put first edge [" + tuple.srcId + "] -> [" + tuple.dstId + " ] , " + tuple.attr + "\n")
      }
    }
    else if (edClass.equals(classOf[Long])) {
      //      buffer.writeLong(firstSrcId)
      //      buffer.writeLong(firstDstid)
      //      buffer.writeLong(firstAttr.asInstanceOf[Long])
      //      writer.write("put first edge [" + firstSrcId + "] -> [" + firstDstid + " ] , " + firstAttr + "\n")
      while (iter.hasNext){
        val tuple = iter.next()
        buffer.writeLong(tuple.srcId)
        buffer.writeLong(tuple.dstId)
        buffer.writeLong(tuple.attr.asInstanceOf[Long])
        //          writer.write("put first edge [" + tuple.srcId + "] -> [" + tuple.dstId + " ] , " + tuple.attr + "\n")
      }
    }
    else if (edClass.equals(classOf[java.lang.Double])) {
      //      buffer.writeLong(firstSrcId)
      //      buffer.writeLong(firstDstid)
      //      buffer.writeDouble(firstAttr.asInstanceOf[java.lang.Double])
      //      writer.write("put first edge [" + firstSrcId + "] -> [" + firstDstid + " ] , " + firstAttr + "\n")
      while (iter.hasNext){
        val tuple = iter.next()
        buffer.writeLong(tuple.srcId)
        buffer.writeLong(tuple.dstId)
        buffer.writeDouble(tuple.attr.asInstanceOf[java.lang.Double])
        //          writer.write("put first edge [" + tuple.srcId + "] -> [" + tuple.dstId + " ] , " + tuple.attr + "\n")
      }
    }
    else if (edClass.equals(classOf[Double])) {
      //      buffer.writeLong(firstSrcId)
      //      buffer.writeLong(firstDstid)
      //      buffer.writeDouble(firstAttr.asInstanceOf[Double])
      //      writer.write("put first edge [" + firstSrcId + "] -> [" + firstDstid + " ] , " + firstAttr + "\n")
      while (iter.hasNext){
        val tuple = iter.next()
        buffer.writeLong(tuple.srcId)
        buffer.writeLong(tuple.dstId)
        buffer.writeDouble(tuple.attr.asInstanceOf[Double])
        //          writer.write("put first edge [" + tuple.srcId + "] -> [" + tuple.dstId + " ] , " + tuple.attr + "\n")
      }
    }
    else if (edClass.equals(classOf[java.lang.Integer])) {
      //      buffer.writeLong(firstSrcId)
      //      buffer.writeLong(firstDstid)
      //      buffer.writeInt(firstAttr.asInstanceOf[java.lang.Integer])
      //      writer.write("put first edge [" + firstSrcId + "] -> [" + firstDstid + " ] , " + firstAttr + "\n")
      while (iter.hasNext){
        val tuple = iter.next()
        buffer.writeLong(tuple.srcId)
        buffer.writeLong(tuple.dstId)
        buffer.writeInt(tuple.attr.asInstanceOf[java.lang.Integer])
        //          writer.write("put first edge [" + tuple.srcId + "] -> [" + tuple.dstId + " ] , " + tuple.attr + "\n")
      }
    }
    else if (edClass.equals(classOf[Int])) {
      //      buffer.writeLong(firstSrcId)
      //      buffer.writeLong(firstDstid)
      //      buffer.writeInt(firstAttr.asInstanceOf[Int])
      //      writer.write("put first edge [" + firstSrcId + "] -> [" + firstDstid + " ] , " + firstAttr + "\n")
      while (iter.hasNext){
        val tuple = iter.next()
        buffer.writeLong(tuple.srcId)
        buffer.writeLong(tuple.dstId)
        buffer.writeInt(tuple.attr.asInstanceOf[Int])
        //          writer.write("put first edge [" + tuple.srcId + "] -> [" + tuple.dstId + " ] , " + tuple.attr + "\n")
      }
    }
    else throw new IllegalStateException("Unexpected ed class " + edClass.getName)
  }

  def class2Int(value: Class[_]): Int = {
    if (value.equals(classOf[java.lang.Long]) || value.equals(classOf[Long])) {
      4
    }
    else if (value.equals(classOf[java.lang.Integer]) || value.equals(classOf[Int])) {
      2
    }
    else if (value.equals(classOf[java.lang.Double]) || value.eq(classOf[Double])) {
      7
    }
    else throw new IllegalArgumentException(s"unexpected class ${value}")
  }

} // end of class Pregel

//    val graphXConf : GraphXConf[VD,ED,A]= new GraphXConf(classTag[VD].runtimeClass.asInstanceOf[java.lang.Class[VD]],
//      classTag[ED].runtimeClass.asInstanceOf[java.lang.Class[ED]],
//      classTag[A].runtimeClass.asInstanceOf[java.lang.Class[A]])
//    val converter = new GraphConverter[VD,ED](classTag[VD].runtimeClass, classTag[ED].runtimeClass)
//    converter.init(graph)
//    val frag =  converter.convert()
//    log.info("convert res: " + frag)
//    val graphxProxy = new GraphXProxy[VD,ED,A](graphXConf,messageManager,comm)
//
//    //Preparations before do all super steps.
//    graphxProxy.beforeApp(vprog.asInstanceOf[(java.lang.Long, VD,A) => VD],
//      sendMsg.asInstanceOf[EdgeTriplet[VD, ED] => Iterator[(java.lang.Long, A)]], mergeMsg, frag, initialMsg)
//
//    //do super steps until converge.
//    graphxProxy.compute()
//
//    //post running stuffs
//    graphxProxy.postApp()
//val randomAccessFile = new RandomAccessFile(strName, "rw")
////      val channel = FileChannel.open(randomAccessFile.toPath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)
//val channel = randomAccessFile.getChannel
//val buffer = channel.map(MapMode.READ_WRITE, 0, MAPPED_SIZE)
