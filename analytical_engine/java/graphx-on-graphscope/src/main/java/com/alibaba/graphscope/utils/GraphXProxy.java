package com.alibaba.graphscope.utils;

import com.alibaba.graphscope.conf.GraphXConf;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.factory.GraphXFactory;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graph.EdgeContextImpl;
import com.alibaba.graphscope.graph.GraphXVertexIdManager;
import com.alibaba.graphscope.graph.GraphxEdgeManager;
import com.alibaba.graphscope.graph.VertexDataManager;
import com.alibaba.graphscope.mm.MessageStore;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.message.DoubleMsg;
import com.alibaba.graphscope.parallel.message.LongMsg;
import org.apache.spark.graphx.EdgeTriplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Function2;
import scala.Function3;
import scala.Tuple2;
import scala.collection.Iterator;

public class GraphXProxy<VD, ED, MSG_T> {

    private static Logger logger = LoggerFactory.getLogger(GraphXProxy.class.getName());
    private static String SPARK_LAUNCHER_OUTPUT = "spark_laucher_output";
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
    private GraphXVertexIdManager idManager;
    private VertexDataManager<VD> vertexDataManager;
    private MessageStore<MSG_T,VD> inComingMessageStore, outgoingMessageStore;
    private EdgeContextImpl<VD, ED, MSG_T> edgeContext;
    private GraphXConf<VD, ED, MSG_T> conf;
    private GraphxEdgeManager<VD, ED, MSG_T> edgeManager;
    private DefaultMessageManager messageManager;
    private IFragment<Long, Long, VD, ED> graphxFragment; // different from c++ frag
    private MSG_T initialMessage;

    public GraphXProxy(GraphXConf<VD, ED, MSG_T> conf, Function3<Long, VD, MSG_T, VD> vprog,
        Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG_T>>> sendMsg,
        Function2<MSG_T, MSG_T, MSG_T> mergeMsg) {
        this.conf = conf;
        this.vprog = vprog;
        this.sendMsg = sendMsg;
        this.mergeMsg = mergeMsg;
        //create all objects here, but not initialized, initialize after main invoke and graph got.
        this.idManager = GraphXFactory.createIdManager(conf);
        this.vertexDataManager = GraphXFactory.createVertexDataManager(conf);
        this.inComingMessageStore = GraphXFactory.createMessageStore(conf);
        this.outgoingMessageStore = GraphXFactory.createMessageStore(conf);
        this.edgeContext = GraphXFactory.createEdgeContext(conf);
        this.edgeManager = GraphXFactory.createEdgeManager(conf, idManager, vertexDataManager);
    }

    public void init(IFragment<Long, Long, VD, ED> fragment, DefaultMessageManager messageManager,
        MSG_T initialMessage) {
        this.graphxFragment = fragment;
        this.messageManager = messageManager;
        this.initialMessage = initialMessage;

        idManager.init(graphxFragment);
        vertexDataManager.init(graphxFragment);
        inComingMessageStore.init(graphxFragment, idManager,vertexDataManager, mergeMsg);
        outgoingMessageStore.init(graphxFragment,  idManager, vertexDataManager,mergeMsg);
        edgeContext.init(outgoingMessageStore);
        edgeManager.init(graphxFragment);
    }

    public void PEval() {
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        long innerVerticesNum = this.graphxFragment.getInnerVerticesNum();
        for (long lid = 0; lid < innerVerticesNum; ++lid) {
            vertexDataManager.setVertexData(lid,
                vprog.apply(idManager.lid2Oid(lid), vertexDataManager.getVertexData(lid),
                    initialMessage));
        }
        logger.info("[PEval] Finish vprog for frag {}", graphxFragment.fid());

        for (long lid = 0; lid < innerVerticesNum; ++lid) {
            edgeContext.setSrcValues(idManager.lid2Oid(lid), lid,
                vertexDataManager.getVertexData(lid));
            edgeManager.iterateOnEdges(lid, edgeContext, sendMsg, outgoingMessageStore);
        }
        logger.info("[PEval] Finish iterate edges for frag {}", graphxFragment.fid());
        outgoingMessageStore.flushMessage(messageManager);
        //messages to self are cached locally.
        outgoingMessageStore.swap(inComingMessageStore);
    }

    public void IncEval() {
        outgoingMessageStore.swap(inComingMessageStore);
        Vertex<Long> receiveVertex = FFITypeFactoryhelper.newVertexLong();
        boolean outerMsgReceived = receiveMessage(receiveVertex);
        long innerVerticesNum = this.graphxFragment.getInnerVerticesNum();

        outgoingMessageStore.clear();
        if (outerMsgReceived || inComingMessageStore.hasMessages()) {
            int vprogCnt = 0;
            for (long lid = 0; lid < innerVerticesNum; ++lid) {
                if (inComingMessageStore.messageAvailable(lid)) {
                    vertexDataManager.setVertexData(lid, vprog.apply(idManager.lid2Oid(lid),
                        vertexDataManager.getVertexData(lid),
                        inComingMessageStore.getMessage(lid)));
                    vprogCnt += 1;
                }
            }
            logger.info("frag {} vprog runned for {} times", graphxFragment.fid(), vprogCnt);

            int sendMsgCnt = 0;
            //after running vprog, we now send msg and merge msg
            for (long lid = 0; lid < innerVerticesNum; ++lid) {
                if (inComingMessageStore.messageAvailable(lid)) {
                    edgeContext.setSrcValues(idManager.lid2Oid(lid), lid,
                        vertexDataManager.getVertexData(lid));
                    edgeManager.iterateOnEdges(lid, edgeContext, sendMsg, outgoingMessageStore);
                    sendMsgCnt += 1;
                }
            }
            logger.info("frag {} vprog runned for {} times", graphxFragment.fid(), sendMsgCnt);

            inComingMessageStore.clear();
            //FIXME: flush message
            outgoingMessageStore.flushMessage(messageManager);
        } else {
            logger.info("Frag {} No message received", graphxFragment.fid());
        }
    }

    public void postApp() {
        logger.info("Post app");
    }

    /**
     * To receive message from grape, we need some wrappers. double -> DoubleMessage. long ->
     * LongMessage
     *
     * @param receiveVertex vertex
     * @return true if message received.
     */
    private boolean receiveMessage(Vertex<Long> receiveVertex) {
        int msgReceived = 0;
        //receive message
        if (conf.getEdataClass().equals(Double.class) || conf.getEdataClass()
            .equals(double.class)) {
            DoubleMsg msg = FFITypeFactoryhelper.newDoubleMsg();
            while (messageManager.getMessage(graphxFragment, receiveVertex, msg)) {
             //   logger.info("get message: {}, {}", receiveVertex.GetValue(), msg.getData());
                inComingMessageStore.addLidMessage(receiveVertex.GetValue(),
                    (MSG_T) (Double) msg.getData());
                msgReceived += 1;
            }
        } else if (conf.getEdataClass().equals(Long.class) || conf.getEdataClass()
            .equals(long.class)) {
            LongMsg msg = FFITypeFactoryhelper.newLongMsg();
            while (messageManager.getMessage(graphxFragment, receiveVertex, msg)) {
             //   logger.info("get message: {}, {}", receiveVertex.GetValue(), msg.getData());
                inComingMessageStore.addLidMessage(receiveVertex.GetValue(),
                    (MSG_T) (Long) msg.getData());
                msgReceived += 1;
            }
        } else {
            logger.info("Not supported msg type");
        }
        logger.info("frag {} received msg from others {}", graphxFragment.fid(), msgReceived);
        return msgReceived > 0;
    }

    public VertexDataManager<VD> getVertexDataManager(){
        return vertexDataManager;
    }

    public MessageStore<MSG_T,VD> getOutgoingMessageStore(){
        return outgoingMessageStore;
    }
}
