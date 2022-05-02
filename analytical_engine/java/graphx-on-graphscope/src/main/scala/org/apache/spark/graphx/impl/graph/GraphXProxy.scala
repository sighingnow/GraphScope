package org.apache.spark.graphx.impl.graph


import com.alibaba.graphscope.ds.Vertex
import com.alibaba.graphscope.fragment.IFragment
import com.alibaba.graphscope.graphx.{GSEdgeTriplet, SharedMemoryRegistry}
import com.alibaba.graphscope.parallel.DefaultMessageManager
import com.alibaba.graphscope.utils.{FFITypeFactoryhelper, MappedBuffer, TypeUtils}
import org.apache.spark.graphx.impl.message.DefaultMessageStore
import org.apache.spark.graphx.traits.{MessageStore, VertexDataManager, GraphXVertexIdManager}
import org.apache.spark.graphx.{EdgeTriplet, GraphXConf, GraphXFactory, VertexId}
import org.apache.spark.internal.Logging

import java.util.Objects
import java.util.concurrent.{CountDownLatch, ExecutorService, Executors}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.Iterator
import scala.reflect.ClassTag

class GraphXProxy[VD : ClassTag, ED : ClassTag, MSG_T: ClassTag](val conf: GraphXConf[VD, ED],
                                                       val fragment : IFragment[Long,Long,_,_],
                                                       val messageManager: DefaultMessageManager,
                                                       val initialMessage: MSG_T,
                                                       val vprog: (VertexId, VD, MSG_T) => VD,
                                                       var sendMsg:EdgeTriplet[VD, ED] => Iterator[(VertexId, MSG_T)],
                                                       var mergeMsg: (MSG_T,MSG_T) => MSG_T,
                                                       val maxIterations: Int,
                                                       var numCores: Int, val vdataPath: String, val vdataSize: Long) extends Logging{
  private var round = 0
  private var vprogTime = 0L
  private var msgSendTime = 0L
  private var receiveTime = 0L
  private var flushTime = 0L
  private val vertexChunkSize = 4096
  private val edgeChunkSize = 1024

  val vdataBuffer: MappedBuffer = SharedMemoryRegistry.getOrCreate.mapFor(vdataPath, vdataSize)
  log.info(s"mapped vdata buffer ${vdataBuffer} of size ${vdataSize}")

  val executorService: ExecutorService = Executors.newFixedThreadPool(numCores)

  val idManager: GraphXVertexIdManager = GraphXFactory.createVertexIdManager(conf, fragment)
  val vertexDataManager: VertexDataManager[VD] = GraphXFactory.createVertexDataManager[VD,ED](conf, fragment)
  val inComingMessageStore: DefaultMessageStore[VD,MSG_T] = GraphXFactory.createDefaultMessageStore[VD,MSG_T](conf, fragment, idManager, vertexDataManager,mergeMsg)
  val outgoingMessageStore: DefaultMessageStore[VD,MSG_T] = GraphXFactory.createDefaultMessageStore[VD,MSG_T](conf,fragment, idManager, vertexDataManager,mergeMsg)

    val edgeTriplets = new Array[GSEdgeTriplet[VD, ED]](numCores)
    for (i <- 0 until numCores) {
      this.edgeTriplets(i) = GraphXFactory.createEdgeTriplet(conf)
    }
    val edgeManager: EdgeManagerImpl[VD, ED] = GraphXFactory.createEdgeManager(conf,fragment, idManager, vertexDataManager, numCores)


  def ParallelPEval(): Unit = {
    vprogTime -= System.nanoTime

    val innerVerticesNum = fragment.getInnerVerticesNum.toInt
    val vprogInteger = new AtomicInteger(0)
    val vprogLatch = new CountDownLatch(numCores)
    for (tid <- 0 until numCores) {
      executorService.execute(new PEvalVprog(tid, vprogInteger, innerVerticesNum, vprogLatch))
    }
    try{
      vprogLatch.await()
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        executorService.shutdown()
    }
    vprogTime += System.nanoTime

    /*--------------send msg ----------------*/
    msgSendTime -= System.nanoTime
    val msgSendInteger = new AtomicInteger(0)
    val msgSendLatch = new CountDownLatch(numCores)
    for (tid <- 0 until numCores) {
      val threadTriplet = edgeTriplets(tid)
      executorService.execute(new PEvalSendMsg(tid, msgSendInteger, innerVerticesNum, msgSendLatch, threadTriplet))
    }
    try{
      msgSendLatch.await()
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        executorService.shutdown()
    }
    msgSendTime += System.nanoTime

    /*--------------flush msg ----------------*/
    flushTime -= System.nanoTime
    outgoingMessageStore.flushMessage(messageManager)
    flushTime += System.nanoTime
    //messages to self are cached locally.
    round = 1
  }
  class PEvalVprog(val tid : Int, val atomicInteger: AtomicInteger, ivNum : Int, countDownLatch: CountDownLatch) extends Runnable{
    override def run(): Unit = {
      while (true){
        val curBegin = Math.min(atomicInteger.getAndAdd(vertexChunkSize), ivNum)
        val curEnd = Math.min(curBegin + vertexChunkSize, ivNum)
        if (curBegin >= curEnd){
          countDownLatch.countDown()
          return
        }
        var lid = curBegin
        while (lid < curEnd) {
          vertexDataManager.setVertexData(lid, vprog.apply(idManager.lid2Oid(lid), vertexDataManager.getVertexData(lid), initialMessage))
          lid += 1
        }
      }
    }
  }
  class PEvalSendMsg(tid: Int, atomicInteger: AtomicInteger, ivNum: Int, latch: CountDownLatch, triplet: GSEdgeTriplet[VD,ED]) extends Runnable{
    override def run(): Unit = {
      while (true) {
        val curBegin = Math.min(atomicInteger.getAndAdd(edgeChunkSize), ivNum)
        val curEnd = Math.min(curBegin + edgeChunkSize, ivNum)
        if (curBegin >= curEnd){
          latch.countDown()
          return
        }
        var lid = curBegin
        while (lid < curEnd) {
          triplet.setSrcOid(idManager.lid2Oid(lid), vertexDataManager.getVertexData(lid))
          edgeManager.iterateOnEdgesParallel(tid, lid, triplet, sendMsg, outgoingMessageStore)
          lid += 1
        }
      }
    }
  }

  def ParallelIncEval: Boolean = {
    receiveTime -= System.nanoTime
    if (round >= maxIterations) {
      return true
    }
    val receiveVertex : Vertex[Long] = FFITypeFactoryhelper.newVertexLong.asInstanceOf[Vertex[Long]]
    val innerVerticesNum = this.fragment.getInnerVerticesNum.toInt

    inComingMessageStore.clear()
    outgoingMessageStore.swap(inComingMessageStore)
    val outerMsgReceived = receiveMessage(receiveVertex.asInstanceOf[Vertex[java.lang.Long]])
    outgoingMessageStore.clear()
    receiveTime += System.nanoTime

    if (outerMsgReceived || inComingMessageStore.hasMessages) {
      vprogTime -= System.nanoTime
      val vprogInteger = new AtomicInteger(0)
      val vprogLatch = new CountDownLatch(numCores)

      for (tid <- 0 until numCores){
        executorService.execute(new IncEvalVprog(tid, vprogInteger,innerVerticesNum, vprogLatch))
      }
      try{
        vprogLatch.await()
      }
      catch {
        case e: Exception =>
          e.printStackTrace()
          executorService.shutdown()
      }
      vprogTime += System.nanoTime
      /*---------------send msg ----------------*/
      msgSendTime -= System.nanoTime
      val msgSendInteger = new AtomicInteger(0)
      val msgSendLatch = new CountDownLatch(numCores)
      for (tid <- 0 until numCores) {
        val threadTriplet = edgeTriplets(tid)
        executorService.execute(new IncEvalSendMsg(tid, msgSendInteger, innerVerticesNum, msgSendLatch, threadTriplet))
      }
      try{
        msgSendLatch.await()
      }
      catch {
        case e: Exception =>
          e.printStackTrace()
          executorService.shutdown()
      }
      msgSendTime += System.nanoTime
      flushTime -= System.nanoTime
      //FIXME: flush message
      outgoingMessageStore.flushMessage(messageManager)
      flushTime += System.nanoTime
    }
    else {
      log.info(s"Frag [${fragment.fid()}] No message received")
    }
    round += 1
    log.info(s"[frag ${fragment.fid()} Profiling]: end of round ${round}, receiveMsg cost ${receiveTime / 1000000}ms, vprog cost ${vprogTime / 1000000}ms, sendMsg cost ${ msgSendTime / 1000000} flush msg ${flushTime / 1000000}")
    false
  }

  class IncEvalVprog(val tid : Int, val atomicInteger: AtomicInteger, ivNum : Int, countDownLatch: CountDownLatch) extends Runnable{
    override def run(): Unit = {
      while (true){
        val curBegin = Math.min(atomicInteger.getAndAdd(vertexChunkSize), ivNum)
        val curEnd = Math.min(curBegin + vertexChunkSize, ivNum)
        if (curBegin >= curEnd){
          countDownLatch.countDown()
          return
        }
        var lid = curBegin
        while (lid < curEnd) {
          if (inComingMessageStore.messageAvailable(lid)) {
            val oid = idManager.lid2Oid(lid)
            val vdata = vertexDataManager.getVertexData(lid)
            val msg = inComingMessageStore.getMessage(lid)
            vertexDataManager.setVertexData(lid, vprog.apply(oid, vdata, msg))
          }
          lid += 1
        }
      }
    }
  }
  class IncEvalSendMsg(tid: Int, atomicInteger: AtomicInteger, ivNum: Int, latch: CountDownLatch, triplet: GSEdgeTriplet[VD,ED]) extends Runnable{
    override def run(): Unit = {
      while (true) {
        val curBegin = Math.min(atomicInteger.getAndAdd(edgeChunkSize), ivNum)
        val curEnd = Math.min(curBegin + edgeChunkSize, ivNum)
        if (curBegin >= curEnd){
          latch.countDown()
          return
        }
        var lid = curBegin
        while (lid < curEnd) {
          if (inComingMessageStore.messageAvailable(lid)) {
            triplet.setSrcOid(idManager.lid2Oid(lid), vertexDataManager.getVertexData(lid))
            edgeManager.iterateOnEdgesParallel(tid, lid, triplet, sendMsg, outgoingMessageStore)
          }
          lid += 1
        }
      }
    }
  }

  def persistVdata(): Unit = {
    val innerVertexNum = fragment.getInnerVerticesNum.toInt
    val bytesToWrite = innerVertexNum * (8 + TypeUtils.classToBytes(conf.getVdClass))
    if (Objects.nonNull(vdataBuffer) && vdataBuffer.remaining >= bytesToWrite) {
      log.info("Writing vertex data to graphx shared memory")
      vdataBuffer.writeLong(bytesToWrite)
      if (conf.getVdClass.equals(classOf[Long]) || conf.getVdClass.equals(classOf[java.lang.Long])) {
        for (lid <- 0 until innerVertexNum) {
          vdataBuffer.writeLong(idManager.lid2Oid(lid))
          vdataBuffer.writeLong(vertexDataManager.getVertexData(lid).asInstanceOf[VD].asInstanceOf[Long])
          log.info(s"Writing ${idManager.lid2Oid(lid)}, ${vertexDataManager.getVertexData(lid)}")
        }
        log.info("Finish writing long vdata to shared memory")
      }
      else if (conf.getVdClass.equals(classOf[Double]) || conf.getVdClass.equals(classOf[java.lang.Double])) {
        for (lid <- 0 until innerVertexNum) {
          vdataBuffer.writeLong(idManager.lid2Oid(lid))
          vdataBuffer.writeDouble(vertexDataManager.getVertexData(lid).asInstanceOf[Double])
        }
        log.info("Finish write double vdata to shared memory")
      }
      else if (conf.getVdClass.equals(classOf[Int]) || conf.getVdClass.equals(classOf[java.lang.Integer])) {
        for (lid <- 0 until innerVertexNum) {
          vdataBuffer.writeLong(idManager.lid2Oid(lid))
          vdataBuffer.writeInt(vertexDataManager.getVertexData(lid).asInstanceOf[VD].asInstanceOf[Integer])
        }
        log.info("Finish write int vdata to shared memory")
      }
      else log.error("Not recognized vdata class: " + conf.getVdClass)
    }
    else log.error(s"vdata null or space not enough ${vdataBuffer}, remaining ${vdataBuffer.remaining}, to write {bytesToWrite}")
  }

  def postApp(): Unit = {
    log.info("Post app")
  }

  /**
   * To receive message from grape, we need some wrappers. double -> DoubleMessage. long ->
   * LongMessage
   *
   * @param receiveVertex vertex
   * @return true if message received.
   */
  private def receiveMessage(receiveVertex: Vertex[java.lang.Long]) : Boolean = {
    var msgReceived = 0
    //receive message
    if (conf.getEdClass.equals(classOf[Double]) || conf.getEdClass.equals(classOf[java.lang.Double])) {
      val msg = FFITypeFactoryhelper.newDoubleMsg
      while ( {
        messageManager.getMessage(fragment, receiveVertex, msg)
      }) { //logger.info("frag {} get message: {}, {}", graphxFragment.fid(), receiveVertex.GetValue(), msg.getData());
        inComingMessageStore.addLidMessage(receiveVertex.GetValue, msg.getData.asInstanceOf[Double].asInstanceOf[MSG_T])
        msgReceived += 1
      }
    }
    else if (conf.getEdClass.equals(classOf[Long]) || conf.getEdClass.equals(classOf[java.lang.Long])) {
      val msg = FFITypeFactoryhelper.newLongMsg
      while ( {
        messageManager.getMessage(fragment, receiveVertex, msg)
      }) {
        inComingMessageStore.addLidMessage(receiveVertex.GetValue, msg.getData.asInstanceOf[MSG_T])
        msgReceived += 1
      }
    }
    else log.info("Not supported msg type ${conf.getEdClass.getName}")
    log.info(s"Frag [${fragment.fid}] received msg from others ${msgReceived}")
    msgReceived > 0
  }

  def getVertexDataManager: VertexDataManager[VD] = vertexDataManager

  def getOutgoingMessageStore: MessageStore[MSG_T] = outgoingMessageStore
}

