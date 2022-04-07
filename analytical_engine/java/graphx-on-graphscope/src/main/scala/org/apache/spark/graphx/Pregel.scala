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
import com.alibaba.graphscope.parallel.DefaultMessageManager
import com.alibaba.graphscope.utils.{CallUtils, MPIProcessLauncher}
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.spark.{SparkContext, graphx}
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils
import org.slf4j.Logger

import java.io.{BufferedWriter, File, FileWriter, RandomAccessFile}
import scala.reflect.{ClassTag, classTag}
import java.nio.CharBuffer
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption
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
object Pregel extends Logging {
  val MAPPED_SIZE = 500 * 1024 * 1024; //500 MB.
  var comm: Communicator = null
  var messageManager: DefaultMessageManager = null

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
   *                        A.  ''This function must be commutative and associative and
   *                        ideally the size of A should not increase.''
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

    val res = graph.vertices.mapPartitionsWithIndex((pid, iterator) => {
      val loggerFileName = "/tmp/graphx-log-" + pid
      val bufferedWriter = new BufferedWriter(new FileWriter(new File(loggerFileName)))
      val strName = s"/tmp/graphx-${pid}"
      val randomAccessFile = new File(strName, "rw")
      val channel = FileChannel.open(randomAccessFile.toPath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)
      val buffer = channel.map(MapMode.READ_WRITE, 0, MAPPED_SIZE)
      //To put vd and ed in the header.
      putHeader(buffer, classTag[VD].runtimeClass.asInstanceOf[java.lang.Class[VD]], classTag[ED].runtimeClass.asInstanceOf[java.lang.Class[ED]], classTag[A].runtimeClass.asInstanceOf[java.lang.Class[A]]);
      bufferedWriter.write("successfully put header + \n")
      putVertices(buffer, iterator,classTag[VD].runtimeClass.asInstanceOf[java.lang.Class[VD]])
      bufferedWriter.write("successfully put data " + buffer.limit() + ", " + buffer.position());
      buffer.compact()
//      iterator
      bufferedWriter.close()
      Iterator(buffer)
    },
      true
    )
    log.info(s"after writing to memory mapped file, launch mpi processes ${res}")

//    graph.vertices.sparkContext.getCallSite()
//    val sc = SparkContext.getOrCreate()
//    val callSite = sc.getCallSite

//    val callsite = Utils.getCallSite()
    val userClass = CallUtils.getCallerCallerClassName
    log.info(s"call site ${userClass}")
    val mpiLauncher = new MPIProcessLauncher("/tmp/graphx-", userClass)
    mpiLauncher.run()

    graph
  } // end of apply

  def putHeader[VD: ClassTag, ED : ClassTag, A: ClassTag](buffer: MappedByteBuffer, vdClass: Class[VD], edClass: Class[ED], msgClass: Class[A]): Unit ={
    require(buffer.remaining() > 4, s"not enough space in buffer")
    buffer.putInt(class2Int(vdClass))
    require(buffer.remaining() > 4, s"not enough space in buffer")
    buffer.putInt(class2Int(edClass))
    require(buffer.remaining() > 4, s"not enough space in buffer")
    buffer.putInt(class2Int(msgClass))
  }

  def putVertices[VD: ClassTag](buffer: MappedByteBuffer, tuples: Iterator[(graphx.VertexId, VD)], vdClass : Class[VD]): Unit ={
    if (vdClass.equals(classOf[java.lang.Long])){
      tuples.foreach(tuple => {
        val vid = tuple._1
        val vdata = tuple._2
        require(buffer.position() > 0 && buffer.position() < buffer.limit(), "buffer position error")
        buffer.putLong(vid)
        buffer.putLong(vdata.asInstanceOf[java.lang.Long])
        log.info(s"Writing vid [${vid}] vdata [${vdata}]")
      })
    }
    else if (vdClass.equals(classOf[java.lang.Double])){
      tuples.foreach(tuple => {
        val vid = tuple._1
        val vdata = tuple._2
        require(buffer.position() > 0 && buffer.position() < buffer.limit(), "buffer position error")
        buffer.putLong(vid)
        buffer.putDouble(vdata.asInstanceOf[java.lang.Double])
        log.info(s"Writing vid [${vid}] vdata [${vdata}]")
      })
    }
    else if (vdClass.equals(classOf[java.lang.Integer])){
      tuples.foreach(tuple => {
        val vid = tuple._1
        val vdata = tuple._2
        require(buffer.position() > 0 && buffer.position() < buffer.limit(), "buffer position error")
        buffer.putLong(vid)
        buffer.putInt(vdata.asInstanceOf[java.lang.Integer])
        log.info(s"Writing vid [${vid}] vdata [${vdata}]")
      })
    }
    else throw new IllegalStateException("expected vdata class")
  }

  def class2Int(value: Class[_]) : Int = {
    if (value.equals(classOf[java.lang.Long])){
      0
    }
    else if (value.equals(classOf[java.lang.Integer])){
      1
    }
    else if (value.equals(classOf[java.lang.Double])){
      2
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
