package com.alibaba.graphscope.utils;

import com.alibaba.graphscope.conf.GraphXConf;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.factory.GraphXFactory;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graph.EdgeContextImpl;
import com.alibaba.graphscope.graph.GraphXVertexIdManager;
import com.alibaba.graphscope.graph.GraphxEdgeManager;
import com.alibaba.graphscope.graph.VertexDataManager;
import com.alibaba.graphscope.graphx.GSEdgeTriplet;
import com.alibaba.graphscope.mm.MessageStore;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.parallel.message.DoubleMsg;
import com.alibaba.graphscope.parallel.message.LongMsg;
import java.util.BitSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
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
    private static final int chunkSize = 4096;
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
//    private EdgeContextImpl<VD, ED, MSG_T> edgeContext;
    private GSEdgeTriplet<VD,ED> edgeTriplet;
    private GSEdgeTriplet<VD,ED>[] edgeTriplets;
    private GraphXConf<VD, ED, MSG_T> conf;
    private GraphxEdgeManager<VD, ED, MSG_T> edgeManager;
    private DefaultMessageManager messageManager;
    private IFragment<Long, Long, VD, ED> graphxFragment; // different from c++ frag
    private MSG_T initialMessage;
    private ExecutorService executorService;
    private int numCores, maxIterations, round;
    private long vprogTime, msgSendTime, receiveTime, flushTime;

    public GraphXProxy(GraphXConf<VD, ED, MSG_T> conf, Function3<Long, VD, MSG_T, VD> vprog,
        Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG_T>>> sendMsg,
        Function2<MSG_T, MSG_T, MSG_T> mergeMsg, int numCores) {
        this.numCores = numCores;
        this.conf = conf;
        this.vprog = vprog;
        this.sendMsg = sendMsg;
        this.mergeMsg = mergeMsg;
        //create all objects here, but not initialized, initialize after main invoke and graph got.
        this.idManager = GraphXFactory.createIdManager(conf);
        this.vertexDataManager = GraphXFactory.createVertexDataManager(conf);
        //fixme: parallel
        this.inComingMessageStore = GraphXFactory.createDefaultMessageStore(conf);
        this.outgoingMessageStore = GraphXFactory.createDefaultMessageStore(conf);
//        this.edgeContext = GraphXFactory.createEdgeContext(conf);
        this.edgeTriplet = GraphXFactory.createEdgeTriplet(conf);
        this.edgeTriplets = new GSEdgeTriplet[numCores];
        for (int i = 0; i < numCores; ++i){
            this.edgeTriplets[i] = GraphXFactory.createEdgeTriplet(conf);
        }
        this.edgeManager = GraphXFactory.createEdgeManager(conf, idManager, vertexDataManager);
        executorService = Executors.newFixedThreadPool(numCores);
    }

    public void init(IFragment<Long, Long, VD, ED> fragment, DefaultMessageManager messageManager,
        MSG_T initialMessage, int maxIterations) {
        this.graphxFragment = fragment;
        this.messageManager = messageManager;
        this.initialMessage = initialMessage;
        this.maxIterations = maxIterations;

        idManager.init(graphxFragment);
        vertexDataManager.init(graphxFragment);
        inComingMessageStore.init(graphxFragment, idManager,vertexDataManager, mergeMsg);
        outgoingMessageStore.init(graphxFragment,  idManager, vertexDataManager,mergeMsg);
//        edgeContext.init(outgoingMessageStore);
        //edgeTriplet no initialization
        edgeManager.init(graphxFragment, numCores);
        round = 0;
        msgSendTime = vprogTime = receiveTime = flushTime = 0;
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
//            edgeContext.setSrcValues(idManager.lid2Oid(lid), lid,
//                vertexDataManager.getVertexData(lid));
            edgeTriplet.setSrcOid(idManager.lid2Oid(lid), vertexDataManager.getVertexData(lid));
            edgeManager.iterateOnEdges(lid, edgeTriplet, sendMsg, outgoingMessageStore);
        }
        logger.info("[PEval] Finish iterate edges for frag {}", graphxFragment.fid());
        outgoingMessageStore.flushMessage(messageManager);
        //messages to self are cached locally.
        round = 1;
    }

    public void ParallelPEval() {
        {
            vprogTime -= System.nanoTime();
            int innerVerticesNum = (int) this.graphxFragment.getInnerVerticesNum();
            AtomicInteger atomicInteger = new AtomicInteger(0);
            CountDownLatch countDownLatch = new CountDownLatch(numCores);
            int originEnd = innerVerticesNum;
            for (int tid = 0; tid < numCores; ++tid) {
                executorService.execute(
                    () -> {
                        while (true) {
                            int curBegin =
                                Math.min(atomicInteger.getAndAdd(chunkSize), originEnd);
                            int curEnd = Math.min(curBegin + chunkSize, originEnd);
                            if (curBegin >= originEnd) {
                                break;
                            }
                            try {
                                for (long lid = curBegin; lid < curEnd; ++lid) {
                                    vertexDataManager.setVertexData(lid,
                                        vprog.apply(idManager.lid2Oid(lid), vertexDataManager.getVertexData(lid),
                                            initialMessage));
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        countDownLatch.countDown();
                    });
            }
            try {
                countDownLatch.await();
            } catch (Exception e) {
                e.printStackTrace();
                executorService.shutdown();
            }
            vprogTime += System.nanoTime();
        }
        {
            msgSendTime -= System.nanoTime();
            int innerVerticesNum = (int) this.graphxFragment.getInnerVerticesNum();
            AtomicInteger atomicInteger = new AtomicInteger(0);
            CountDownLatch countDownLatch = new CountDownLatch(numCores);
            int originEnd = innerVerticesNum;
            for (int tid = 0; tid < numCores; ++tid) {
                GSEdgeTriplet<VD,ED> threadTriplet = edgeTriplets[tid];
                int finalTid = tid;
                executorService.execute(
                    () -> {
                        while (true) {
                            int curBegin =
                                Math.min(atomicInteger.getAndAdd(chunkSize), originEnd);
                            int curEnd = Math.min(curBegin + chunkSize, originEnd);
                            if (curBegin >= originEnd) {
                                break;
                            }
                            try {
                                for (long lid = curBegin; lid < curEnd; ++lid) {
                                    threadTriplet.setSrcOid(idManager.lid2Oid(lid), vertexDataManager.getVertexData(lid));
                                    edgeManager.iterateOnEdgesParallel(finalTid, lid, threadTriplet, sendMsg, outgoingMessageStore);
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        countDownLatch.countDown();
                    });
            }
            try {
                countDownLatch.await();
            } catch (Exception e) {
                e.printStackTrace();
                executorService.shutdown();
            }
            msgSendTime += System.nanoTime();
        }

        flushTime -= System.nanoTime();
        outgoingMessageStore.flushMessage(messageManager);
        flushTime += System.nanoTime();
        //messages to self are cached locally.
        round = 1;

    }

    public void IncEval() {
        if (round >= maxIterations){
            return ;
        }
        outgoingMessageStore.swap(inComingMessageStore);
        Vertex<Long> receiveVertex = FFITypeFactoryhelper.newVertexLong();
        long innerVerticesNum = this.graphxFragment.getInnerVerticesNum();

        inComingMessageStore.clear();
        outgoingMessageStore.swap(inComingMessageStore);
        boolean outerMsgReceived = receiveMessage(receiveVertex);
        outgoingMessageStore.clear();
        if (outerMsgReceived || inComingMessageStore.hasMessages()) {
            for (long lid = 0; lid < innerVerticesNum; ++lid) {
                if (inComingMessageStore.messageAvailable(lid)) {
                    vertexDataManager.setVertexData(lid, vprog.apply(idManager.lid2Oid(lid),
                        vertexDataManager.getVertexData(lid),
                        inComingMessageStore.getMessage(lid)));
                }
            }

            int sendMsgCnt = 0;
            //after running vprog, we now send msg and merge msg
            for (long lid = 0; lid < innerVerticesNum; ++lid) {
                if (inComingMessageStore.messageAvailable(lid)) {
                    edgeTriplet.setSrcOid(idManager.lid2Oid(lid), vertexDataManager.getVertexData(lid));
                    edgeManager.iterateOnEdges(lid, edgeTriplet, sendMsg, outgoingMessageStore);
                    sendMsgCnt += 1;
                }
            }
            logger.info("frag {} vprog runned for {} times", graphxFragment.fid(), sendMsgCnt);

            //FIXME: flush message
            outgoingMessageStore.flushMessage(messageManager);
        } else {
            logger.info("Frag {} No message received", graphxFragment.fid());
        }
        round += 1;
    }

    public boolean ParallelIncEval() {
        receiveTime -= System.nanoTime();
        if (round >= maxIterations) {
            return true;
        }
        Vertex<Long> receiveVertex = FFITypeFactoryhelper.newVertexLong();
        long innerVerticesNum = this.graphxFragment.getInnerVerticesNum();


        inComingMessageStore.clear();
        outgoingMessageStore.swap(inComingMessageStore);
        boolean outerMsgReceived = receiveMessage(receiveVertex);
        outgoingMessageStore.clear();
        receiveTime += System.nanoTime();

        BitSet flags = inComingMessageStore.getFlags();
        int []activeVertices = new int [flags.cardinality()];
        int index = 0;
        int i = flags.nextSetBit(0);
        for (;i >= 0 && index < activeVertices.length; i = flags.nextSetBit(i + 1)){
            if (i == Integer.MAX_VALUE){
                break;
            }
            activeVertices[index++] = i;
        }
        if (i < 0){
            throw new IllegalStateException("bit set still available: " + i + " index: " + index + " length: " + activeVertices.length);
        }
        logger.info("total vnum: " + innerVerticesNum + " cardinality: " + flags.cardinality() + " active vertices: " + activeVertices.length);

        if (outerMsgReceived || inComingMessageStore.hasMessages()) {
            {
                vprogTime -= System.nanoTime();
                AtomicInteger atomicInteger = new AtomicInteger(0);
                CountDownLatch countDownLatch = new CountDownLatch(numCores);
                int originEnd = (int) innerVerticesNum;
                for (int tid = 0; tid < numCores; ++tid) {
                    executorService.execute(
                        () -> {
                            while (true) {
                                int curBegin =
                                    Math.min(atomicInteger.getAndAdd(chunkSize), originEnd);
                                int curEnd = Math.min(curBegin + chunkSize, originEnd);
                                if (curBegin >= originEnd) {
                                    break;
                                }
                                try {
                                    for (long lid = curBegin; lid < curEnd; ++lid) {
                                        if (inComingMessageStore.messageAvailable(lid)) {
                                            vertexDataManager.setVertexData(lid, vprog.apply(idManager.lid2Oid(lid),
                                                vertexDataManager.getVertexData(lid),
                                                inComingMessageStore.getMessage(lid)));
                                        }
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                            countDownLatch.countDown();
                        });
                }
                try {
                    countDownLatch.await();
                } catch (Exception e) {
                    e.printStackTrace();
                    executorService.shutdown();
                }
                vprogTime += System.nanoTime();
            }

            {
                msgSendTime -= System.nanoTime();
                AtomicInteger atomicInteger = new AtomicInteger(0);
                int originEnd = activeVertices.length;
                int edgeChunkSize = Math.max(256, activeVertices.length / numCores);
                int curCores = activeVertices.length / edgeChunkSize;
                CountDownLatch countDownLatch = new CountDownLatch(curCores);
                for (int tid = 0; tid < curCores; ++tid) {
                    GSEdgeTriplet<VD,ED> threadTriplet = edgeTriplets[tid];
                    int finalTid = tid;
                    executorService.execute(
                        () -> {
                            if (atomicInteger.get() >= originEnd) return ;
                            while (true) {
                                int curBegin =
                                    Math.min(atomicInteger.getAndAdd(edgeChunkSize), originEnd);
                                int curEnd = Math.min(curBegin + edgeChunkSize, originEnd);
                                if (curBegin >= originEnd) {
                                    break;
                                }
                                try {
                                    for (int j = curBegin; j < curEnd; ++j) {
                                        long lid = activeVertices[j];
                                        if (inComingMessageStore.messageAvailable(lid)) {
                                            threadTriplet.setSrcOid(idManager.lid2Oid(lid), vertexDataManager.getVertexData(lid));
                                            edgeManager.iterateOnEdgesParallel(finalTid, lid, threadTriplet, sendMsg, outgoingMessageStore);
                                        }
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                            countDownLatch.countDown();
                        });
                }
                try {
                    countDownLatch.await();
                } catch (Exception e) {
                    e.printStackTrace();
                    executorService.shutdown();
                }
                msgSendTime += System.nanoTime();
            }

            flushTime -= System.nanoTime();
            //FIXME: flush message
            outgoingMessageStore.flushMessage(messageManager);
            flushTime += System.nanoTime();
        } else {
            logger.info("Frag {} No message received", graphxFragment.fid());
        }
        round += 1;
        logger.info("[frag {} Profiling]: end of round {}, receiveMsg cost {}ms, vprog cost {}ms, sendMsg cost {} flush msg {}",graphxFragment.fid(), round, receiveTime/ 1000000, vprogTime /1000000, msgSendTime / 1000000, flushTime / 1000000);
        return false;
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
                //logger.info("frag {} get message: {}, {}", graphxFragment.fid(), receiveVertex.GetValue(), msg.getData());
                inComingMessageStore.addLidMessage(receiveVertex.GetValue(),
                    (MSG_T) (Double) msg.getData());
                msgReceived += 1;
            }
        } else if (conf.getEdataClass().equals(Long.class) || conf.getEdataClass()
            .equals(long.class)) {
            LongMsg msg = FFITypeFactoryhelper.newLongMsg();
            while (messageManager.getMessage(graphxFragment, receiveVertex, msg)) {
                //logger.info("frag {} get message: {}, {}", graphxFragment.fid(), receiveVertex.GetValue(), msg.getData());
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
