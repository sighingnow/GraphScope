package com.alibaba.graphscope.graph;

import com.alibaba.graphscope.ds.MutableTypedArray;
import com.alibaba.graphscope.ds.PropertyNbrUnit;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.GraphXFragment;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.fragment.adaptor.GraphXFragmentAdaptor;
import com.alibaba.graphscope.graphx.GSEdgeTriplet;
import com.alibaba.graphscope.graphx.GSEdgeTripletImpl;
import com.alibaba.graphscope.graphx.SerializationUtils;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.message.DoubleMsg;
import com.alibaba.graphscope.parallel.message.LongMsg;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import java.util.BitSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.spark.graphx.EdgeTriplet;
import org.apache.spark.graphx.GraphXConf;
import org.apache.spark.graphx.GraphXFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Function2;
import scala.Function3;
import scala.Tuple2;
import scala.collection.Iterator;

public class GraphXPIE<VD,ED,MSG_T> {
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
    private Function2<MSG_T, MSG_T, MSG_T> mergeMsg;
    private IFragment<Long, Long, VD, ED> iFragment; // different from c++ frag
    private GraphXFragment<Long,Long,VD,ED> graphXFragment;
    private MSG_T initialMessage;
    private ExecutorService executorService;
    private int numCores, maxIterations, round;
    private long vprogTime, msgSendTime, receiveTime, flushTime;
    private GraphXConf<VD,ED,MSG_T> conf;
    private GSEdgeTriplet<VD,ED>[] edgeTriplets;
    private GSEdgeTripletImpl<VD,ED> edgeTriplet;
    DefaultMessageManager messageManager;
    private MutableTypedArray<ED> edataArray; //Indexed with eid
    private MutableTypedArray<VD> vdataArray;
    private long innerVerticesNum, verticesNum;
    private BitSet bitSet;

    public MutableTypedArray<VD> getVdataArray() {return vdataArray;}

    public GraphXPIE(GraphXConf<VD, ED, MSG_T> conf, String vprogPath, String sendMsgPath, String mergeMsgPath) {
        this.conf = conf;
        try {
            this.vprog = (Function3<Long, VD, MSG_T, VD>) SerializationUtils.read(vprogPath);
            this.sendMsg = (Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG_T>>>) SerializationUtils.read(
                sendMsgPath);
            this.mergeMsg = (Function2<MSG_T, MSG_T, MSG_T>) SerializationUtils.read(mergeMsgPath);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        this.edgeTriplet = new GSEdgeTripletImpl<>();
    }

    public GraphXPIE(GraphXConf<VD, ED, MSG_T> conf, Function3<Long, VD, MSG_T, VD> vprog,
        Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG_T>>> sendMsg,
        Function2<MSG_T, MSG_T, MSG_T> mergeMsg) {
        this.conf = conf;
        this.vprog = vprog;
        this.sendMsg = sendMsg;
        this.mergeMsg = mergeMsg;
        this.edgeTriplet = new GSEdgeTripletImpl<>();
    }

    public void init(IFragment<Long, Long, VD, ED> fragment, DefaultMessageManager messageManager,
        MSG_T initialMessage, int maxIterations) {
        this.iFragment = fragment;
        if (iFragment.fragmentType() != GraphXFragmentAdaptor.fragmentType){
            throw new IllegalStateException("Only support graphx fragment");
        }
        this.graphXFragment = ((GraphXFragmentAdaptor<Long,Long,VD,ED>) iFragment).getGraphXFragment();
        vdataArray = graphXFragment.getVdataArray();
        edataArray = graphXFragment.getEdataArray();
        logger.info("edata array size {}, edge num{}", edataArray.getLength(), graphXFragment.getEdgeNum());
        if (edataArray.getLength() != graphXFragment.getEdgeNum()){
            throw new IllegalStateException("not equal" + edataArray.getLength() + ","+ graphXFragment.getEdgeNum());
        }
        logger.info("vdata array size {}, frag vnum{}", vdataArray.getLength(), graphXFragment.getVerticesNum());
        if (vdataArray.getLength() != graphXFragment.getVerticesNum()){
            throw new IllegalStateException("not equal" + vdataArray.getLength() + ","+ graphXFragment.getVerticesNum());
        }
        this.messageManager = messageManager;
        this.initialMessage = initialMessage;
        this.maxIterations = maxIterations;
        innerVerticesNum = graphXFragment.getInnerVerticesNum();
        verticesNum = graphXFragment.getVerticesNum();
        bitSet = new BitSet((int) verticesNum);
        round = 0;
        msgSendTime = vprogTime = receiveTime = flushTime = 0;
    }

    public void PEval() {
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        for (long lid = 0; lid < innerVerticesNum; ++lid) {
            vertex.SetValue(lid);
            Long oid = graphXFragment.getId(vertex);
            VD originalVD = vdataArray.get(lid);
            graphXFragment.setData(vertex, vprog.apply(oid, originalVD, initialMessage));
            logger.info("Running vprog on {}, oid {}, original vd {}, cur vd {}", lid, graphXFragment.getId(vertex), originalVD, vdataArray.get(lid));
        }

        for (long lid = 0; lid < innerVerticesNum; ++lid) {
            vertex.SetValue(lid);
            Long oid = graphXFragment.getId(vertex);
            edgeTriplet.setSrcOid(oid, vdataArray.get(lid));
            iterateOnEdges(vertex, edgeTriplet);
        }
        logger.info("[PEval] Finish iterate edges for frag {}", graphXFragment.fid());
        flushOutMessage();
        round = 1;
    }

    void flushOutMessage(){
        Vertex<Long> v = FFITypeFactoryhelper.newVertexLong();
        for (int i = bitSet.nextSetBit((int) innerVerticesNum); i >= 0; i = bitSet.nextSetBit(i + 1)){
            v.SetValue((long) i);
            messageManager.syncStateOnOuterVertexGraphX(graphXFragment, v, vdataArray.get(i));
            logger.info("Frag {} send msg {} to outer v {}", graphXFragment.fid(), vdataArray.get(i), v.GetValue());
        }
        bitSet.clear();
    }
    void iterateOnEdges(Vertex<Long> vertex, GSEdgeTripletImpl<VD,ED> edgeTriplet){
        PropertyNbrUnit<Long> begin = graphXFragment.getBegin(vertex);
        PropertyNbrUnit<Long> end = graphXFragment.getEnd(vertex);
        int cnt = 0;
        Vertex<Long> nbrVertex = FFITypeFactoryhelper.newVertexLong();
        while (begin.getAddress() != end.getAddress()){
            logger.info("Visiting edge {} of vertex {}", cnt, vertex.GetValue());
            Long nbrVid = begin.vid();
            nbrVertex.SetValue(nbrVid);
            edgeTriplet.setDstOid(graphXFragment.getId(nbrVertex), vdataArray.get(nbrVid));
            edgeTriplet.setAttr(edataArray.get(begin.eid()));
            Iterator<Tuple2<Long, MSG_T>> msgs = sendMsg.apply(edgeTriplet);
            logger.info("for edge: {}({}) -> {}({}), edge attr {}", edgeTriplet.srcId(), edgeTriplet.srcAttr(), edgeTriplet.dstId(), edgeTriplet.dstAttr(), edgeTriplet.attr);
            while (msgs.hasNext()){
                Tuple2<Long, MSG_T> msg = msgs.next();
                graphXFragment.getVertex(msg._1, vertex);
                logger.info("Oid {} to vertex {}", msg._1, vertex.GetValue());

                //FIXME: currently we assume msg type equal to vdata type
                MSG_T original_MSG = (MSG_T) vdataArray.get(vertex.GetValue());
                VD res = (VD) mergeMsg.apply(original_MSG, msg._2);
                logger.info("Merge msg ori {} new {} res {}", original_MSG, msg._2, res);
                vdataArray.set(vertex.GetValue(),res);

                if (vertex.GetValue() >= innerVerticesNum) {
                    bitSet.set(Math.toIntExact(vertex.GetValue()));
                }
            }
            begin.nextV();
            cnt += 1;
        }
    }

    public boolean IncEval() {
        if (round >= maxIterations){
            return true;
        }
        boolean outerMsgReceived = receiveMessage();
        if (outerMsgReceived) {
            Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
            for (int i = bitSet.nextSetBit(0); i >= 0 ; i = bitSet.nextSetBit(i + 1)){
                if (i >= innerVerticesNum){
                    throw new IllegalStateException("Not possible to receive a msg send to outer vertex");
                }
                vertex.SetValue((long) i);
                Long oid = graphXFragment.getId(vertex);
                VD originalVD = vdataArray.get(i);
                logger.info("Running vprog on {}, oid {}, original vd {}, cur vd {}", i, graphXFragment.getId(vertex), originalVD,vdataArray.get(i));
            }
            bitSet.clear();
            for (long lid = 0; lid < innerVerticesNum; ++lid) {
                vertex.SetValue(lid);
                Long oid = graphXFragment.getId(vertex);
                edgeTriplet.setSrcOid(oid, vdataArray.get(lid));
                iterateOnEdges(vertex, edgeTriplet);
            }
            logger.info("[IncEval {}] Finish iterate edges for frag {}",round, graphXFragment.fid());
            flushOutMessage();
            round +=1;
        } else {
            logger.info("Frag {} No message received", graphXFragment.fid());
            return true;
        }
        round += 1;
        return false;
    }


    public void postApp() {
        logger.info("Post app");
    }

    /**
     * To receive message from grape, we need some wrappers. double -> DoubleMessage. long ->
     * LongMessage
     *
     * @return true if message received.
     */
    private boolean receiveMessage() {
        Vertex<Long> receiveVertex = FFITypeFactoryhelper.newVertexLong();
        int msgReceived = 0;
        bitSet.clear();
        //receive message
        if (conf.getMsgClass().equals(Double.class) || conf.getMsgClass()
            .equals(double.class)) {
            DoubleMsg msg = FFITypeFactoryhelper.newDoubleMsg();
            while (messageManager.getMessageGraphX(graphXFragment, receiveVertex, msg, 2.0)) {
                if (receiveVertex.GetValue() >= verticesNum){
                    throw new IllegalStateException("Receive illegal vertex " + receiveVertex.GetValue() +" msg: "+ msg.getData());
                }
                vdataArray.set(receiveVertex.GetValue(), (VD)(Double)msg.getData());
                msgReceived += 1;
                bitSet.set(receiveVertex.GetValue().intValue());
            }
        } else if (conf.getMsgClass().equals(Long.class) || conf.getMsgClass()
            .equals(long.class)) {
            LongMsg msg = FFITypeFactoryhelper.newLongMsg();
            while (messageManager.getMessageGraphX(graphXFragment, receiveVertex, msg, 2.0)) {
                if (receiveVertex.GetValue() >= verticesNum){
                    throw new IllegalStateException("Receive illegal vertex " + receiveVertex.GetValue() +" msg: "+ msg.getData());
                }
                vdataArray.set(receiveVertex.GetValue(), (VD)(Long)msg.getData());
                msgReceived += 1;
                bitSet.set(receiveVertex.GetValue().intValue());
            }
        } else {
            logger.info("Not supported msg type");
        }
        logger.info("frag {} received msg from others {}", graphXFragment.fid(), msgReceived);
        return msgReceived > 0;
    }
}
