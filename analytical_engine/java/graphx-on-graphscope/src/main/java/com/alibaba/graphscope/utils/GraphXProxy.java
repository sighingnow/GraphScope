package com.alibaba.graphscope.utils;

import com.alibaba.graphscope.communication.Communicator;
import com.alibaba.graphscope.conf.GraphXConf;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.factory.GraphXFactory;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedAdaptor;
import com.alibaba.graphscope.graph.EdgeContextImpl;
import com.alibaba.graphscope.graph.GraphXVertexIdManager;
import com.alibaba.graphscope.graph.GraphxEdgeManager;
import com.alibaba.graphscope.graph.VertexDataManager;
import com.alibaba.graphscope.mm.MessageStore;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
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
    private MessageStore<MSG_T> inComingMessageStore, outgoingMessageStore;
    private EdgeContextImpl<VD, ED, MSG_T> edgeContext;
    private GraphXConf<VD, ED, MSG_T> conf;
    private Communicator communicator;
    private GraphxEdgeManager<VD, ED, MSG_T> edgeManager;
    private DefaultMessageManager messageManager;
    private IFragment<Long, Long, VD, ED> graphxFragment; // different from c++ frag
    private MSG_T initialMessage;

    public GraphXProxy(GraphXConf<VD, ED, MSG_T> conf, DefaultMessageManager messageManager,
        Communicator communicator) {
        this.conf = conf;
        this.messageManager = messageManager;
        this.communicator = communicator;
        //create all objects here, but not initialized, initialize after main invoke and graph got.
        this.idManager = GraphXFactory.createIdManager(conf);
        this.vertexDataManager = GraphXFactory.createVertexDataManager(conf);
        this.inComingMessageStore = GraphXFactory.createMessageStore(conf);
        this.outgoingMessageStore = GraphXFactory.createMessageStore(conf);
        this.edgeContext = GraphXFactory.createEdgeContext(conf);
        this.edgeManager = GraphXFactory.createEdgeManager(conf, idManager, vertexDataManager);
    }

    public void beforeApp(Function3<Long, VD, MSG_T, VD> vprog,
        Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG_T>>> sendMsg,
        Function2<MSG_T, MSG_T, MSG_T> mergeMsg, ArrowProjectedFragment<Long, Long, VD, ED> frag,
        MSG_T initialMessage) {
        this.vprog = vprog;
        this.sendMsg = sendMsg;
        this.mergeMsg = mergeMsg;
        this.graphxFragment = new ArrowProjectedAdaptor<Long, Long, VD, ED>(frag);
        this.initialMessage = initialMessage;

        idManager.init(graphxFragment);
        vertexDataManager.init(graphxFragment);
        inComingMessageStore.init(graphxFragment, idManager, mergeMsg);
        outgoingMessageStore.init(graphxFragment,idManager, mergeMsg);
        edgeContext.init(outgoingMessageStore);
        edgeManager.init(graphxFragment);
    }

    public void compute() {

        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        long innerVerticesNum = this.graphxFragment.getInnerVerticesNum();
        long totalTime = -System.nanoTime();
        long t = -System.nanoTime();
        for (long lid = 0; lid < innerVerticesNum; ++lid) {
            vertexDataManager.setVertexData(lid,vprog.apply(idManager.lid2Oid(lid), vertexDataManager.getVertexData(lid),
                initialMessage));
        }

        for (long lid = 0; lid < innerVerticesNum; ++lid) {
            edgeContext.setSrcValues(idManager.lid2Oid(lid), lid,
                vertexDataManager.getVertexData(lid));
            edgeManager.iterateOnEdges(lid, edgeContext, sendMsg, outgoingMessageStore);
        }
        //FIXME: flush message
        outgoingMessageStore.flushMessage(messageManager);

        t += System.nanoTime();
        logger.info("PEval Finished: [{}]", t / 10e9);
        //IN default_worker.h, Already enter PEval,
        messageManager.FinishARound();

        Vertex<Long> receiveVertex = FFITypeFactoryhelper.newVertexLong();

        int round = 1;
        while (true) {
            messageManager.StartARound();
            t = -System.nanoTime();
            //receive message
            MSG_T msg = null;
            try {
                msg = conf.getMsgClass().newInstance();
            } catch (Exception e) {
                e.printStackTrace();
            }
            inComingMessageStore.beforeSuperStep();
            outgoingMessageStore.beforeSuperStep();

            boolean msgReceived = false;
            while (messageManager.getMessage(graphxFragment, receiveVertex, msg)) {
                logger.info("get message: {}, {}", receiveVertex.GetValue(), msg);
                inComingMessageStore.addLidMessage(receiveVertex.GetValue(), msg);
                msgReceived = true;
            }
            if (msgReceived) {
                for (long lid = 0; lid < innerVerticesNum; ++lid) {
                    if (inComingMessageStore.messageAvailable(lid)) {
                        vprog.apply(idManager.lid2Oid(lid), vertexDataManager.getVertexData(lid),
                            inComingMessageStore.getMessage(lid));
                    }
                }
                //after running vprog, we now send msg and merge msg
                for (long lid = 0; lid < innerVerticesNum; ++lid) {
                    edgeContext.setSrcValues(idManager.lid2Oid(lid), lid,
                        vertexDataManager.getVertexData(lid));
                    edgeManager.iterateOnEdges(lid, edgeContext, sendMsg, outgoingMessageStore);
                }
                //FIXME: flush message
                outgoingMessageStore.flushMessage(messageManager);

                //after send message, flush message in message manager.
                t += System.nanoTime();
                logger.info("IncEval [{}], query time [{}]:", round, t / 10e9);
                messageManager.FinishARound();
            } else {
                logger.info("Frag {} No message received in round {}", graphxFragment.fid(), round);
                break;
            }
        }
        totalTime += System.nanoTime();
        logger.info("Total time: [{}]", totalTime);
    }

    public void postApp() {
        logger.info("Post app");
    }

}
