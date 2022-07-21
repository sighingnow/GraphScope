package com.alibaba.graphscope.graphx;

import com.alibaba.graphscope.ds.ImmutableTypedArray;
import com.alibaba.graphscope.ds.PropertyNbrUnit;
import com.alibaba.graphscope.ds.StringTypedArray;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.BaseGraphXFragment;
import com.alibaba.graphscope.fragment.FragmentType;
import com.alibaba.graphscope.fragment.GraphXFragment;
import com.alibaba.graphscope.fragment.GraphXStringEDFragment;
import com.alibaba.graphscope.fragment.GraphXStringVDFragment;
import com.alibaba.graphscope.fragment.GraphXStringVEDFragment;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.fragment.adaptor.GraphXFragmentAdaptor;
import com.alibaba.graphscope.fragment.adaptor.GraphXStringEDFragmentAdaptor;
import com.alibaba.graphscope.fragment.adaptor.GraphXStringVDFragmentAdaptor;
import com.alibaba.graphscope.fragment.adaptor.GraphXStringVEDFragmentAdaptor;
import com.alibaba.graphscope.graphx.graph.GSEdgeTripletImpl;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.stdcxx.FFIByteVectorFactory;
import com.alibaba.graphscope.stdcxx.StdVector;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.alibaba.graphscope.utils.MessageStore;
import com.alibaba.graphscope.utils.TriConsumer;
import com.alibaba.graphscope.utils.array.PrimitiveArray;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.BitSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import jnr.ffi.annotations.In;
import org.apache.spark.graphx.EdgeDirection;
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
    private static int BATCH_SIZE = 4096;
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
    protected BaseGraphXFragment<Long, Long, VD, ED> graphXFragment;
    private MSG_T initialMessage;
    private ExecutorService executorService;
    private int numCores, maxIterations, round;
    private long vprogTime, msgSendTime, receiveTime, flushTime;
    private GraphXConf<VD, ED, MSG_T> conf;
    //    private GSEdgeTripletImpl<VD, ED> edgeTriplet;
    DefaultMessageManager messageManager;
    //FIXME: for primitive ones, can we avoid copying?
    private PrimitiveArray<VD> newVdataArray;
    private PrimitiveArray<ED> newEdataArray;
    /**
     * The messageStore stores the result messages after query. 1) Before PEval or IncEval,
     * messageStore should be clear. 2) After iterateOnEdges, we need to flush messages a. For
     * message to inner vertex, set them locally b. For message to outer vertex, send via mpi 3)
     * after flush, clear message store.
     */
    private MessageStore<MSG_T> messageStore;
    private int innerVerticesNum, verticesNum;
    private BitSet curSet, nextSet;
    private EdgeDirection direction;
    private long[] lid2Oid;
    private PropertyNbrUnit<Long>[] nbrs;
    private long oeBeginAddress, ieBeginAddress;
    private ImmutableTypedArray<Long> oeOffsetArray, ieOffsetArray;
    private int[] fid2WorkerId;

    public PrimitiveArray<VD> getNewVdataArray() {
        return newVdataArray;
    }

    public GraphXPIE(GraphXConf<VD, ED, MSG_T> conf, Function3<Long, VD, MSG_T, VD> vprog,
        Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG_T>>> sendMsg,
        Function2<MSG_T, MSG_T, MSG_T> mergeMsg, MSG_T initialMessage, EdgeDirection direction) {
        this.conf = conf;
        this.vprog = vprog;
        this.sendMsg = sendMsg;
        this.mergeMsg = mergeMsg;
//        this.
        this.initialMessage = initialMessage;
        this.direction = direction;
    }

    public void init(IFragment<Long, Long, VD, ED> fragment, DefaultMessageManager messageManager,
        int maxIterations, int parallelism, String workerIdToFid) throws IOException, ClassNotFoundException {
        this.iFragment = fragment;
        this.numCores = parallelism;
        if (!(iFragment.fragmentType().equals(FragmentType.GraphXFragment)
            || iFragment.fragmentType().equals(FragmentType.GraphXStringVDFragment)
            || iFragment.fragmentType().equals(FragmentType.GraphXStringEDFragment)
            || iFragment.fragmentType().equals(FragmentType.GraphXStringVEDFragment))) {
            throw new IllegalStateException("Only support graphx fragment");
        }
        this.graphXFragment = getBaseGraphXFragment(iFragment);
        Tuple2<PrimitiveArray<VD>, PrimitiveArray<ED>> tuple = initOldAndNewArrays(graphXFragment,
            conf);
        newVdataArray = tuple._1();
        newEdataArray = tuple._2();

        this.messageManager = messageManager;
        this.maxIterations = maxIterations;
        innerVerticesNum = (int) graphXFragment.getInnerVerticesNum();
        verticesNum = graphXFragment.getVerticesNum().intValue();
        this.messageStore = MessageStore.create((int) verticesNum, fragment.fnum(), numCores,
            conf.getMsgClass(), mergeMsg);
        logger.info("ivnum {}, tvnum {}", innerVerticesNum, verticesNum);
        curSet = new BitSet((int) verticesNum);
        //initially activate all vertices
        curSet.set(0, verticesNum);
        nextSet = new BitSet((int) verticesNum);
        round = 0;
        lid2Oid = new long[(int) verticesNum];
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        for (int i = 0; i < verticesNum; ++i) {
            vertex.SetValue((long) i);
            lid2Oid[i] = graphXFragment.getId(vertex);
        }
        vertex.SetValue(0L);
        oeOffsetArray = graphXFragment.getCSR().getOEOffsetsArray();
        ieOffsetArray = graphXFragment.getCSR().getIEOffsetsArray();
        nbrs = new PropertyNbrUnit[parallelism];
        for (int i = 0; i < parallelism; ++i) {
            nbrs[i] = graphXFragment.getOEBegin(vertex);
        }

        oeBeginAddress = graphXFragment.getOEBegin(vertex).getAddress();
        ieBeginAddress = graphXFragment.getIEBegin(vertex).getAddress();

        executorService = Executors.newFixedThreadPool(numCores);
        logger.info("Parallelism for frag {} is {}", graphXFragment.fid(), numCores);
        fid2WorkerId = new int[graphXFragment.fnum()];
        fillFid2WorkerId(workerIdToFid);
        msgSendTime = vprogTime = receiveTime = flushTime = 0;
    }

    private void runVProg(int startLid, int endLid, boolean firstRound) {
        for (int lid = curSet.nextSetBit(startLid); lid >= 0 && lid < endLid;
            lid = curSet.nextSetBit(lid + 1)) {
            long oid = lid2Oid[lid];
            VD originalVD = newVdataArray.get(lid);
            if (firstRound) {
                newVdataArray.set(lid, vprog.apply(oid, originalVD, initialMessage));
            } else {
                if (lid < 5){
                    logger.info("vprog for {}({}) old vd {}, msg{}", lid, oid, originalVD, messageStore.get(lid));
                }
                newVdataArray.set(lid, vprog.apply(oid, originalVD, messageStore.get(lid)));
            }
        }
    }

    private void iterateEdge(int startLid, int endLid, int threadId) {
        GSEdgeTripletImpl<VD, ED> edgeTriplet = new GSEdgeTripletImpl<>();
        PropertyNbrUnit<Long> nbr = nbrs[threadId];
        if (direction.equals(EdgeDirection.Either()) || direction.equals(EdgeDirection.Out())){
            for (int lid = curSet.nextSetBit(startLid); lid >= 0 && lid < endLid;
                lid = curSet.nextSetBit(lid + 1)) {
                long oid = lid2Oid[lid];
                VD vAttr = newVdataArray.get(lid);
                edgeTriplet.setSrcOid(oid, vAttr);
                edgeTriplet.setSrcLid(lid);
                iterateOnOutEdgesImpl(lid, edgeTriplet, nbr,threadId);
            }
        }
/*
        if (direction.equals(EdgeDirection.Either()) || direction.equals(EdgeDirection.In())){
            for (int lid = curSet.nextSetBit(startLid); lid >= 0 && lid < endLid;
                lid = curSet.nextSetBit(lid + 1)) {
                long oid = lid2Oid[lid];
                VD vAttr = newVdataArray.get(lid);
                edgeTriplet.setDstOid(oid, vAttr);
                iterateOnInEdgesImpl(lid, edgeTriplet, nbr);
            }
        }
*/
    }

    public void parallelExecute(TriConsumer<Integer, Integer, Integer> function, int  limit) {
        AtomicInteger getter = new AtomicInteger(0);
        CountDownLatch countDownLatch = new CountDownLatch(numCores);
        for (int tid = 0; tid < numCores; ++tid) {
            final int finalTid = tid;
            executorService.execute(
                () -> {
                    int begin, end;
                    while (true) {
                        begin = Math.min(getter.getAndAdd(BATCH_SIZE), limit);
                        end = Math.min(begin + BATCH_SIZE, limit);
                        if (begin >= end) {
                            break;
                        }
                        function.accept(begin, end, finalTid);
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
    }

    public void ParallelPEval() {
        vprogTime -= System.nanoTime();
            //We need to update outer vertex message to vd array, otherwise, we will send out message
            //infinitely.
        parallelExecute((begin, end, threadId) -> runVProg(begin, end, true), verticesNum);
        vprogTime += System.nanoTime();

        msgSendTime -= System.nanoTime();
        parallelExecute(this::iterateEdge, innerVerticesNum);
        msgSendTime += System.nanoTime();
        logger.info("[PEval] Finish iterate edges for frag {}", graphXFragment.fid());
        flushTime -= System.nanoTime();
        try {
            messageStore.flushMessages(nextSet, messageManager, graphXFragment, fid2WorkerId);
        } catch (IOException e) {
            e.printStackTrace();
        }
//        nextSet.clear((int) innerVerticesNum, (int) verticesNum);
        flushTime += System.nanoTime();
        round = 1;
    }

    public void PEval() {
        vprogTime -= System.nanoTime();
        runVProg(0, (int) innerVerticesNum, true);
        vprogTime += System.nanoTime();

        msgSendTime -= System.nanoTime();
        iterateEdge(0, (int) innerVerticesNum, 1);
        msgSendTime += System.nanoTime();
        logger.info("[PEval] Finish iterate edges for frag {}", graphXFragment.fid());
        flushTime -= System.nanoTime();
        try {
            messageStore.flushMessages(nextSet, messageManager, graphXFragment,fid2WorkerId);
        } catch (IOException e) {
            e.printStackTrace();
        }
        nextSet.clear((int) innerVerticesNum, (int) verticesNum);
        flushTime += System.nanoTime();
        round = 1;
    }

    void iterateOnOutEdgesImpl(int lid, GSEdgeTripletImpl<VD, ED> edgeTriplet,
        PropertyNbrUnit<Long> nbr, int threadId) {
        long beginOffset, endOffset;
        beginOffset = oeOffsetArray.get(lid);
        endOffset = oeOffsetArray.get(lid + 1);
        nbr.setAddress(beginOffset * 16 + oeBeginAddress);
        while (beginOffset < endOffset) {
            long nbrVid = nbr.vid();
            edgeTriplet.setDstLid(nbrVid);
            edgeTriplet.setDstOid(lid2Oid[(int) nbrVid], newVdataArray.get(nbrVid));
            edgeTriplet.setAttr(newEdataArray.get(nbr.eid()));
            if (lid < 5){
                logger.info("visiting triplet {}", edgeTriplet);
            }
            Iterator<Tuple2<Long, MSG_T>> msgs = sendMsg.apply(edgeTriplet);
            messageStore.addMessages(msgs, graphXFragment, nextSet, threadId,edgeTriplet);
            nbr.addV(16);
            beginOffset += 1;
        }
    }

    void iterateOnInEdgesImpl(int lid, GSEdgeTripletImpl<VD, ED> edgeTriplet,
        PropertyNbrUnit<Long> nbr, int threadId) {
        long beginOffset, endOffset;
        beginOffset = ieOffsetArray.get(lid);
        endOffset = ieOffsetArray.get(lid + 1);
        nbr.setAddress(beginOffset * 16 + ieBeginAddress);
        while (beginOffset < endOffset) {
            long nbrVid = nbr.vid();
            edgeTriplet.setSrcOid(lid2Oid[(int) nbrVid], newVdataArray.get(nbrVid));
            edgeTriplet.setAttr(newEdataArray.get(nbr.eid()));
            Iterator<Tuple2<Long, MSG_T>> msgs = sendMsg.apply(edgeTriplet);
            messageStore.addMessages(msgs, graphXFragment, nextSet,threadId,edgeTriplet);
            nbr.addV(16);
            beginOffset += 1;
        }
    }

    public boolean ParallelIncEval() {
        if (round >= maxIterations) {
            return true;
        }
        //set nextSet(0, ivnum) to curSet(0, ivnum).
        curSet.clear();
        curSet.or(nextSet);
        nextSet.clear();

        /////////////////////////////////////Receive message////////////////////
        int active0 = curSet.cardinality();
        receiveMessage();
        int active1 = curSet.cardinality();
        logger.info("[IncEval {}]Frag [{}] before receive msg has {} active vertices, after has {} active vertices", round,
            graphXFragment.fid(), active0, active1);

        if (curSet.cardinality() > 0) {
            logger.info("Before running round {}, frag [{}] has {} active vertices", round,
                graphXFragment.fid(), curSet.cardinality());
            vprogTime -= System.nanoTime();
            parallelExecute((begin, end, threadId) -> runVProg(begin, end, false),verticesNum);
            vprogTime += System.nanoTime();

            msgSendTime -= System.nanoTime();
            parallelExecute(this::iterateEdge,innerVerticesNum);
            msgSendTime += System.nanoTime();
            logger.info("[IncEval {}] Finish iterate edges for frag {}", round,
                graphXFragment.fid());
            flushTime -= System.nanoTime();
            try {
                messageStore.flushMessages(nextSet, messageManager, graphXFragment,fid2WorkerId);
            } catch (IOException e) {
                e.printStackTrace();
            }
//            nextSet.clear((int) innerVerticesNum, (int) verticesNum);
            flushTime += System.nanoTime();
        } else {
            logger.info("Frag {} No message received", graphXFragment.fid());
            round += 1;
            return true;
        }
        round += 1;
        logger.info("Round [{}] vprog {}, msgSend {} flushMsg {}", round, vprogTime / 1000000,
            msgSendTime / 1000000, flushTime / 1000000);
        return false;
    }

    public boolean IncEval() {
        if (round >= maxIterations) {
            return true;
        }
        //set nextSet(0, ivnum) to curSet(0, ivnum).
        curSet.clear();
        curSet.or(nextSet);
        nextSet.clear();

        /////////////////////////////////////Receive message////////////////////
        receiveMessage();

        if (curSet.cardinality() > 0) {
            logger.info("Before running round {}, frag [{}] has {} active vertices, ivnum [{}]", round,
                graphXFragment.fid(), curSet.cardinality(), innerVerticesNum);
            vprogTime -= System.nanoTime();
            runVProg(0, (int) innerVerticesNum, false);
            vprogTime += System.nanoTime();

            msgSendTime -= System.nanoTime();
            iterateEdge(0, (int) innerVerticesNum, 1);
            msgSendTime += System.nanoTime();
            logger.info("[IncEval {}] Finish iterate edges for frag {}, active [{}] vertices", round,
                graphXFragment.fid(), nextSet.cardinality());
            flushTime -= System.nanoTime();
            try {
                messageStore.flushMessages(nextSet, messageManager, graphXFragment,fid2WorkerId);
            } catch (IOException e) {
                e.printStackTrace();
            }
            nextSet.clear((int) innerVerticesNum, (int) verticesNum);
            flushTime += System.nanoTime();
            logger.info("[IncEval {}] Finish flush outer vertices of frag {}, active inner vertices [{}]", round, graphXFragment.fid(), nextSet.cardinality());
        } else {
            logger.info("Frag {} No message received", graphXFragment.fid());
            round += 1;
            return true;
        }
        round += 1;
        logger.info("Round [{}] vprog {}, msgSend {} flushMsg {}", round, vprogTime / 1000000,
            msgSendTime / 1000000, flushTime / 1000000);
        return false;
    }

    /**
     * To receive message from grape, we need some wrappers. double -> DoubleMessage. long ->
     * LongMessage. Vprog happens here
     *
     * @return true if message received.
     */
    private void receiveMessage() {
        FFIByteVector tmpVector = (FFIByteVector) FFIByteVectorFactory.INSTANCE.create();
        long bytesOfReceivedMsg = 0;
        while (messageManager.getPureMessage(tmpVector)) {
            // The retrieved tmp vector has been resized, so the cached objAddress is not available.
            // trigger the refresh
            tmpVector.touch();
            // OutArchive will do the resize;
            if (logger.isDebugEnabled()) {
                logger.debug("Frag [{}] digest message of size {}", graphXFragment.fid(),
                    tmpVector.size());
            }
            messageStore.digest(tmpVector, graphXFragment, curSet);
            bytesOfReceivedMsg += tmpVector.size();
        }
        logger.info("Frag [{}] Totally received {} bytes", graphXFragment.fid(),
            bytesOfReceivedMsg);
    }

    private static <VD_T, ED_T, MSG_T_> Tuple2<PrimitiveArray<VD_T>, PrimitiveArray<ED_T>> initOldAndNewArrays(
        BaseGraphXFragment<Long, Long, VD_T, ED_T> baseGraphXFragment,
        GraphXConf<VD_T, ED_T, MSG_T_> conf)
        throws IOException, ClassNotFoundException {
        //For vd array
        if (conf.isVDPrimitive() && conf.isEDPrimitive()) {
            ImmutableTypedArray<VD_T> oldVdataArray;
            ImmutableTypedArray<ED_T> oldEdataArray;
            GraphXFragment<Long, Long, VD_T, ED_T> graphXFragment = (GraphXFragment<Long, Long, VD_T, ED_T>) baseGraphXFragment;
            oldEdataArray = graphXFragment.getEdataArray();
            oldVdataArray = graphXFragment.getVdataArray();
            logger.info("vdata array size {}, frag vnum{}", oldVdataArray.getLength(),
                graphXFragment.getVerticesNum());
            if (oldVdataArray.getLength() != graphXFragment.getVerticesNum()) {
                throw new IllegalStateException("not equal" + oldVdataArray.getLength() + ","
                    + graphXFragment.getVerticesNum());
            }
            PrimitiveArray<ED_T> newEdataArray = processPrimitiveArray(oldEdataArray,
                conf.getEdClass());
            //Should contain outer vertices
            PrimitiveArray<VD_T> newVdataArray = processPrimitiveArray(oldVdataArray,
                conf.getVdClass());
            return new Tuple2<>(newVdataArray, newEdataArray);
        } else if (!conf.isVDPrimitive() && conf.isEDPrimitive()) {
            StringTypedArray oldVdataArray;
            ImmutableTypedArray<ED_T> oldEdataArray;
            GraphXStringVDFragment<Long, Long, VD_T, ED_T> graphXFragment = (GraphXStringVDFragment<Long, Long, VD_T, ED_T>) baseGraphXFragment;
            oldEdataArray = graphXFragment.getEdataArray();
            oldVdataArray = graphXFragment.getVdataArray();
            logger.info("total bytes in vd array {}, vertices num {}", oldVdataArray.getLength(),
                graphXFragment.getVerticesNum());

            PrimitiveArray<ED_T> newEdataArray = processPrimitiveArray(oldEdataArray,
                conf.getEdClass());
            PrimitiveArray<VD_T> newVdataArray = processComplexArray(oldVdataArray,
                conf.getVdClass());
            return new Tuple2<>(newVdataArray, newEdataArray);
        } else if (conf.isVDPrimitive() && !conf.isEDPrimitive()) {
            StringTypedArray oldEDdataArray;
            ImmutableTypedArray<VD_T> oldVdataArray;
            GraphXStringEDFragment<Long, Long, VD_T, ED_T> graphXFragment = (GraphXStringEDFragment<Long, Long, VD_T, ED_T>) baseGraphXFragment;
            oldEDdataArray = graphXFragment.getEdataArray();
            oldVdataArray = graphXFragment.getVdataArray();
            logger.info("total bytes in vd array {}, vertices num {}", oldVdataArray.getLength(),
                graphXFragment.getVerticesNum());

            PrimitiveArray<VD_T> newVdataArray = processPrimitiveArray(oldVdataArray,
                conf.getVdClass());
            PrimitiveArray<ED_T> newEdataArray = processComplexArray(oldEDdataArray,
                conf.getEdClass());
            return new Tuple2<>(newVdataArray, newEdataArray);
        } else if (!conf.isVDPrimitive() && !conf.isEDPrimitive()) {
            StringTypedArray oldEDdataArray;
            StringTypedArray oldVdataArray;
            GraphXStringVEDFragment<Long, Long, VD_T, ED_T> graphXFragment = (GraphXStringVEDFragment<Long, Long, VD_T, ED_T>) baseGraphXFragment;
            oldEDdataArray = graphXFragment.getEdataArray();
            oldVdataArray = graphXFragment.getVdataArray();
            logger.info("total bytes in vd array {}, total bytes in ed array {}, vertices num {}",
                oldVdataArray.getLength(), oldEDdataArray.getLength(),
                graphXFragment.getVerticesNum());

            PrimitiveArray<VD_T> newVdataArray = processComplexArray(oldVdataArray,
                conf.getVdClass());
            PrimitiveArray<ED_T> newEdataArray = processComplexArray(oldEDdataArray,
                conf.getEdClass());
            return new Tuple2<>(newVdataArray, newEdataArray);
        } else {
            logger.error("Not implemented for vd {} ed {}", conf.isVDPrimitive(),
                conf.isEDPrimitive());
            throw new IllegalStateException("not implemented");
        }
    }

    private static <T> PrimitiveArray<T> processComplexArray(StringTypedArray oldArray,
        Class<? extends T> clz)
        throws IOException, ClassNotFoundException {
        StdVector<Byte> data = oldArray.getRawBytes();
        FFIByteVector ffiByteVector = new FFIByteVector(data.getAddress());
        FFIByteVectorInputStream ffiInput = new FFIByteVectorInputStream(ffiByteVector);
        ObjectInputStream objectInputStream = new ObjectInputStream(ffiInput);
        long len = oldArray.getLength();
        logger.info("reading {} objects from array of bytes {}", len, data.size());
        PrimitiveArray<T> newArray = PrimitiveArray.create(clz, (int) len);
        for (int i = 0; i < len; ++i) {
            T obj = (T) objectInputStream.readObject();
            newArray.set(i, obj);
        }
        return newArray;
    }

    private static <T> PrimitiveArray<T> processPrimitiveArray(ImmutableTypedArray<T> oldArray,
        Class<? extends T> clz) {
        PrimitiveArray<T> newArray = PrimitiveArray.create(clz, (int) oldArray.getLength());
        long time0 = System.nanoTime();
        long len = oldArray.getLength();
        for (int i = 0; i < len; ++i) {
            newArray.set(i, oldArray.get(i));
        }
        long time1 = System.nanoTime();
        logger.info("[Coping array cost:  {}ms", (time1 - time0) / 1000000);
        return newArray;
    }

    private BaseGraphXFragment<Long, Long, VD, ED> getBaseGraphXFragment(
        IFragment<Long, Long, VD, ED> iFragment) {
        if (iFragment.fragmentType().equals(FragmentType.GraphXStringVDFragment)) {
            return ((GraphXStringVDFragmentAdaptor<Long, Long, VD, ED>) iFragment).getFragment();
        } else if (iFragment.fragmentType().equals(FragmentType.GraphXFragment)) {
            return ((GraphXFragmentAdaptor<Long, Long, VD, ED>) iFragment).getFragment();
        } else if (iFragment.fragmentType().equals(FragmentType.GraphXStringEDFragment)) {
            return ((GraphXStringEDFragmentAdaptor<Long, Long, VD, ED>) iFragment).getFragment();
        } else if (iFragment.fragmentType().equals(FragmentType.GraphXStringVEDFragment)) {
            return ((GraphXStringVEDFragmentAdaptor<Long, Long, VD, ED>) iFragment).getFragment();
        } else {
            throw new IllegalStateException("Not implemented" + iFragment);
        }
    }
    private void fillFid2WorkerId(String str){
        logger.info("try to parse " + str);
        String[] splited = str.split(";");
        if (splited.length != fid2WorkerId.length){
            throw new IllegalStateException("length neq " + splited.length + "," + fid2WorkerId.length);
        }
        for (String tuple : splited){
            String[] tmp = tuple.split(":");
            if (tmp.length != 2){
                throw new IllegalStateException("length neq 2" + tmp.length);
            }
            int workerId = Integer.parseInt(tmp[0]);
            int fid = Integer.parseInt(tmp[1]);
            fid2WorkerId[fid] = workerId;
        }
    }
}
