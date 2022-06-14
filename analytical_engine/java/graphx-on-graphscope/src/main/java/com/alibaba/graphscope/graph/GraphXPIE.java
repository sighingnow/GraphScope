package com.alibaba.graphscope.graph;

import com.alibaba.graphscope.ds.ImmutableTypedArray;
import com.alibaba.graphscope.ds.PropertyNbrUnit;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.GraphXFragment;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.fragment.adaptor.GraphXFragmentAdaptor;
import com.alibaba.graphscope.graphx.GSEdgeTriplet;
import com.alibaba.graphscope.graphx.GSEdgeTripletImpl;
import com.alibaba.graphscope.graphx.GraphXConf;
import com.alibaba.graphscope.graphx.SerializationUtils;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.message.DoubleMsg;
import com.alibaba.graphscope.parallel.message.LongMsg;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.alibaba.graphscope.utils.array.PrimitiveArray;
import java.net.URLClassLoader;
import java.util.BitSet;
import java.util.concurrent.ExecutorService;
import org.apache.spark.graphx.EdgeTriplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Function2;
import scala.Function3;
import scala.Tuple2;
import scala.collection.Iterator;

public class GraphXPIE<VD, ED, MSG_T> {
  private static Logger logger = LoggerFactory.getLogger(GraphXPIE.class.getName());
  private static final int vertexChunkSize = 4096;
  private static final int edgeChunkSize = 1024;
  /**
   * User vertex program: vprog: (VertexId, VD, A) => VD
   */
  private Function3<Long, VD, MSG_T, VD> vprog;
  /**
   * EdgeTriplet[VD, ED] => Iterator[(VertexId, A)]
   */
  private Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG_T>>> sendMsg;
  /**
   * (A, A) => A)
   */
  protected Function2<MSG_T, MSG_T, MSG_T> mergeMsg;
  protected IFragment<Long, Long, VD, ED> iFragment; // different from c++ frag
  protected GraphXFragment<Long, Long, VD, ED> graphXFragment;
  private MSG_T initialMessage;
  private ExecutorService executorService;
  private int numCores, maxIterations, round;
  private long vprogTime, msgSendTime, receiveTime, flushTime;
  private GraphXConf<VD,ED,MSG_T> conf;
  private GSEdgeTriplet<VD, ED>[] edgeTriplets;
  private GSEdgeTripletImpl<VD, ED> edgeTriplet;
  DefaultMessageManager messageManager;
  private ImmutableTypedArray<ED> oldEdataArray;
  private ImmutableTypedArray<VD> oldVdataArray;
  private PrimitiveArray<VD> newVdataArray;
  private PrimitiveArray<ED> newEdataArray;
  private long innerVerticesNum, verticesNum;
  private BitSet curSet,nextSet;

  public PrimitiveArray<VD> getNewVdataArray() {
    return newVdataArray;
  }

  public GraphXPIE(GraphXConf<VD, ED, MSG_T> conf, Function3<Long, VD, MSG_T, VD> vprog,
                   Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG_T>>> sendMsg,
                   Function2<MSG_T, MSG_T, MSG_T> mergeMsg,MSG_T initialMessage) {
    this.conf = conf;
    this.vprog = vprog;
    this.sendMsg = sendMsg;
    this.mergeMsg = mergeMsg;
    this.edgeTriplet = new GSEdgeTripletImpl<>();
    this.initialMessage = initialMessage;
  }

  public void init(IFragment<Long, Long, VD, ED> fragment, DefaultMessageManager messageManager,
                    int maxIterations) {
    this.iFragment = fragment;
    if (iFragment.fragmentType() != GraphXFragmentAdaptor.fragmentType) {
      throw new IllegalStateException("Only support graphx fragment");
    }
    this.graphXFragment =
        ((GraphXFragmentAdaptor<Long, Long, VD, ED>) iFragment).getGraphXFragment();
    oldEdataArray = graphXFragment.getEdataArray();

    oldVdataArray = graphXFragment.getVdataArray();
    logger.info("vdata array size {}, frag vnum{}", oldVdataArray.getLength(),
                graphXFragment.getVerticesNum());
    if (oldVdataArray.getLength() != graphXFragment.getVerticesNum()) {
      throw new IllegalStateException("not equal" + oldVdataArray.getLength() + ","
                                      + graphXFragment.getVerticesNum());
    }
    /** During query, updates are saved to on-heap array, after calculation, we persist them out*/
    newVdataArray = PrimitiveArray.create(conf.getVdClass(), (int) oldVdataArray.getLength());
    newEdataArray = PrimitiveArray.create(conf.getEdClass(), (int) oldEdataArray.getLength());
    {
      long time0 = System.nanoTime();
      long len = oldEdataArray.getLength();
      for (int i = 0; i < len; ++i) {
        newEdataArray.set(i, oldEdataArray.get(i));
      }
      long time1 = System.nanoTime();
      logger.info("[Coping edata array cost: ] {}ms", (time1 - time0) / 1000000);
    }
    {
      long len = oldVdataArray.getLength();
      for (int i = 0; i < len; ++i){
        newVdataArray.set(i, oldVdataArray.get(i));
      }
    }
    this.messageManager = messageManager;
    this.initialMessage = initialMessage;
    this.maxIterations = maxIterations;
    innerVerticesNum = graphXFragment.getInnerVerticesNum();
    verticesNum = graphXFragment.getVerticesNum();
    logger.info("ivnum {}, tvnum {}", innerVerticesNum, verticesNum);
    curSet = new BitSet((int) verticesNum);
    nextSet = new BitSet((int) verticesNum);
    round = 0;
    msgSendTime = vprogTime = receiveTime = flushTime = 0;
  }

  public void PEval() {
    Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
    vprogTime -= System.nanoTime();
    for (long lid = 0; lid < innerVerticesNum; ++lid) {
      vertex.SetValue(lid);
      Long oid = graphXFragment.getId(vertex);
      VD originalVD = newVdataArray.get(lid);
      newVdataArray.set(lid, vprog.apply(oid, originalVD, initialMessage));
//      logger.info("Running vprog on {}, oid {}, original vd {}, cur vd {}", lid,
//                  graphXFragment.getId(vertex), originalVD, newVdataArray.get(lid));
    }
    vprogTime += System.nanoTime();

    msgSendTime -= System.nanoTime();
    for (long lid = 0; lid < innerVerticesNum; ++lid) {
      vertex.SetValue(lid);
      Long oid = graphXFragment.getInnerVertexId(vertex);
      edgeTriplet.setSrcOid(oid, newVdataArray.get(lid));
      iterateOnEdges(vertex, edgeTriplet);
    }
    msgSendTime += System.nanoTime();
    logger.info("[PEval] Finish iterate edges for frag {}", graphXFragment.fid());
    flushTime -= System.nanoTime();
    flushOutMessage();
    flushTime += System.nanoTime();
    round = 1;
  }

  void flushOutMessage() {
    Vertex<Long> v = FFITypeFactoryhelper.newVertexLong();
    int cnt = 0;
    for (int i = nextSet.nextSetBit((int) innerVerticesNum); i >= 0; i = nextSet.nextSetBit(i + 1)) {
      v.SetValue((long) i);
      messageManager.syncStateOnOuterVertexGraphX(graphXFragment, v, newVdataArray.get(i),
                                                  newVdataArray.get(i));
      //logger.info("frag {} send msg {} to outer vertex {},  dst frag id {}, gid {}", graphXFragment.fid(),newVdataArray.get(i), i, graphXFragment.getFragId(v), graphXFragment.getOuterVertexGid(v));
      cnt += 1;
    }
    logger.info("Frag [{}] try to send {} msg to outer vertices", graphXFragment.fid(), cnt);
    nextSet.clear((int) innerVerticesNum, (int) verticesNum);
  }

  void iterateOnEdges(Vertex<Long> vertex, GSEdgeTripletImpl<VD, ED> edgeTriplet) {
    PropertyNbrUnit<Long> begin = graphXFragment.geOEBegin(vertex);
    PropertyNbrUnit<Long> end = graphXFragment.getOEEnd(vertex);
    int cnt = 0;
    Vertex<Long> nbrVertex = FFITypeFactoryhelper.newVertexLong();
    Vertex<Long> tmpVertex = FFITypeFactoryhelper.newVertexLong();
    while (begin.getAddress() != end.getAddress()) {
//      logger.info("Visiting edge {} of vertex {}(oid {})", cnt, vertex.GetValue(), edgeTriplet.srcId());
      Long nbrVid = begin.vid();
      nbrVertex.SetValue(nbrVid);
      edgeTriplet.setDstOid(graphXFragment.getId(nbrVertex), newVdataArray.get(nbrVid));
      edgeTriplet.setAttr(newEdataArray.get(begin.eid()));
      Iterator<Tuple2<Long, MSG_T>> msgs = sendMsg.apply(edgeTriplet);
//      logger.info("for edge: lid:{} oid:{}({}) -> lid:{} oid {} ({}), edge attr {}, ivnum {}", vertex.GetValue(), edgeTriplet.srcId(),
//                  edgeTriplet.srcAttr(), nbrVid, edgeTriplet.dstId(), edgeTriplet.dstAttr(),
//                  edgeTriplet.attr, innerVerticesNum);
      while (msgs.hasNext()) {
        Tuple2<Long, MSG_T> msg = msgs.next();
        if (!graphXFragment.getVertex(msg._1(), tmpVertex)){
           throw new IllegalStateException("get vertex for oid failed: " + msg._1());
        }
	if (tmpVertex.GetValue() > innerVerticesNum){
//           logger.info("got outer vertex: {}",tmpVertex.GetValue()); 
	}
//        logger.info("Oid {} to vertex {}", msg._1(), tmpVertex.GetValue());

        // FIXME: currently we assume msg type equal to vdata type
        MSG_T original_MSG = (MSG_T) newVdataArray.get(tmpVertex.GetValue());
        VD res = (VD) mergeMsg.apply(original_MSG, msg._2());
//        logger.info("Merge msg ({} + {}) = {}", original_MSG, msg._2(), res);
        newVdataArray.set(tmpVertex.GetValue(), res);
        if (tmpVertex.GetValue() > innerVerticesNum){
            //logger.info("frag {} send {} to outer vertex {}", graphXFragment.fid(), newVdataArray.get(tmpVertex.GetValue()), tmpVertex.GetValue());
        }
        nextSet.set(Math.toIntExact(tmpVertex.GetValue()));
      }
//      begin.nextV();
      begin.addV(16);
      cnt += 1;
    }
  }

  public boolean IncEval() {
    if (round >= maxIterations) {
      return true;
    }
    //set nextSet(0, ivnum) to curSet(0, ivnum).
    curSet.clear();
    curSet.or(nextSet);
    nextSet.clear();
    receiveMessage();
    logger.info("Before running round {}, frag [{}] has {} active vertices", round, graphXFragment.fid(), curSet.cardinality());
    if (curSet.cardinality() > 0) {
      Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();

//      for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
//        if (i >= innerVerticesNum) {
//          throw new IllegalStateException("Not possible to receive a msg send to outer vertex");
//        }
//        vertex.SetValue((long) i);
//        Long oid = graphXFragment.getId(vertex);
//        VD originalVD = newVdataArray.get(i);
//        newVdataArray.set(i, vprog.apply(oid, originalVD, initialMessage));
//      }
      msgSendTime -= System.nanoTime();
      for (int lid = curSet.nextSetBit(0); lid >= 0; lid = curSet.nextSetBit(lid + 1)){
        vertex.SetValue((long) lid);
        Long oid = graphXFragment.getId(vertex);
        edgeTriplet.setSrcOid(oid, newVdataArray.get(lid));
        iterateOnEdges(vertex, edgeTriplet);
      }
      msgSendTime += System.nanoTime();
      logger.info("[IncEval {}] Finish iterate edges for frag {}", round, graphXFragment.fid());
      flushTime -= System.nanoTime();
      flushOutMessage();
      flushTime += System.nanoTime();
    } else {
      logger.info("Round [{}] vprog {}, msgSend {} flushMsg {}", round, vprogTime /1000000, msgSendTime/1000000, flushTime/1000000);
      logger.info("Frag {} No message received", graphXFragment.fid());
      round += 1;
      return true;
    }
    round += 1;
    logger.info("Round [{}] vprog {}, msgSend {} flushMsg {}", round, vprogTime/1000000, msgSendTime/1000000, flushTime/1000000);
    return false;
  }

  public void postApp() {
    logger.info("Post app");
  }

  /**
   * To receive message from grape, we need some wrappers. double -> DoubleMessage. long ->
   * LongMessage.
   * Vprog happens here
   *
   * @return true if message received.
   */
  private void receiveMessage() {
    Vertex<Long> receiveVertex = FFITypeFactoryhelper.newVertexLong();
          vprogTime -= System.nanoTime();
    int msgReceived = 0;
//    bitSet.clear();
    // receive message
    if (conf.getMsgClass().equals(Double.class) || conf.getMsgClass().equals(double.class)) {
      DoubleMsg msg = FFITypeFactoryhelper.newDoubleMsg();
      while (messageManager.getMessageGraphX(graphXFragment, receiveVertex, msg, 2.0)) {
        if (receiveVertex.GetValue() >= innerVerticesNum) {
          throw new IllegalStateException("Receive illegal vertex " + receiveVertex.GetValue()
                                          + " msg: " + msg.getData());
        }
//        newVdataArray.set(receiveVertex.GetValue(), (VD) (Double) msg.getData());
        Long lid = receiveVertex.GetValue();
        newVdataArray.set(lid, vprog.apply(graphXFragment.getId(receiveVertex), newVdataArray.get(lid), (MSG_T) (Double) msg.getData()));
        msgReceived += 1;
        curSet.set(receiveVertex.GetValue().intValue());
      }
    } else if (conf.getMsgClass().equals(Long.class) || conf.getMsgClass().equals(long.class)) {
      LongMsg msg = FFITypeFactoryhelper.newLongMsg();
      while (messageManager.getMessageGraphX(graphXFragment, receiveVertex, msg, 2.0)) {
        if (receiveVertex.GetValue() >= innerVerticesNum) {
          throw new IllegalStateException("Receive illegal vertex " + receiveVertex.GetValue()
                                          + " msg: " + msg.getData());
        }
        Long lid = receiveVertex.GetValue();
        newVdataArray.set(lid, vprog.apply(graphXFragment.getId(receiveVertex), newVdataArray.get(lid), (MSG_T) (Long) msg.getData()));
        msgReceived += 1;
        curSet.set(receiveVertex.GetValue().intValue());
      }
    } else {
      logger.info("Not supported msg type " + conf.getMsgClass().getName());
    }
    vprogTime += System.nanoTime();
    logger.info("frag {} received msg from others {}", graphXFragment.fid(), msgReceived);
//    return msgReceived > 0;
  }
}
