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
import com.alibaba.graphscope.utils.array.PrimitiveArray;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.BitSet;
import java.util.concurrent.ExecutorService;
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
    private GSEdgeTripletImpl<VD, ED> edgeTriplet;
    DefaultMessageManager messageManager;
    private PrimitiveArray<VD> newVdataArray;
    private PrimitiveArray<ED> newEdataArray;
    /**
     * The messageStore stores the result messages after query. 1) Before PEval or IncEval,
     * messageStore should be clear. 2) After iterateOnEdges, we need to flush messages a. For
     * message to inner vertex, set them locally b. For message to outer vertex, send via mpi 3)
     * after flush, clear message store.
     */
    private MessageStore<MSG_T> messageStore;
    private long innerVerticesNum, verticesNum;
    private BitSet curSet, nextSet;
    private EdgeDirection direction;

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
        this.edgeTriplet = new GSEdgeTripletImpl<>();
        this.initialMessage = initialMessage;
        this.direction = direction;
    }

    public void init(IFragment<Long, Long, VD, ED> fragment, DefaultMessageManager messageManager,
        int maxIterations) throws IOException, ClassNotFoundException {
        this.iFragment = fragment;
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
        innerVerticesNum = graphXFragment.getInnerVerticesNum();
        verticesNum = graphXFragment.getVerticesNum();
        this.messageStore = MessageStore.create((int) verticesNum, fragment.fnum(),
            conf.getMsgClass(), mergeMsg);
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
        try {
            messageStore.flushMessages(nextSet, messageManager, graphXFragment);
        } catch (IOException e) {
            e.printStackTrace();
        }
        nextSet.clear((int) innerVerticesNum, (int) verticesNum);
        flushTime += System.nanoTime();
        round = 1;
    }

    void iterateOnEdges(Vertex<Long> vertex, GSEdgeTripletImpl<VD, ED> edgeTriplet){
        if (direction.equals(EdgeDirection.Either())){
            iterateOnEdgesImpl(vertex, edgeTriplet, true);
            iterateOnEdgesImpl(vertex, edgeTriplet, false);
        }
        else if (direction.equals(EdgeDirection.In())){
            iterateOnEdgesImpl(vertex, edgeTriplet, true);
        }
        else if (direction.equals(EdgeDirection.Out())){
            iterateOnEdgesImpl(vertex, edgeTriplet, false);
        }
        else {
            throw new IllegalStateException("edge direction: both is not supported");
        }
    }

    void iterateOnEdgesImpl(Vertex<Long> vertex, GSEdgeTripletImpl<VD, ED> edgeTriplet, boolean inEdge) {
        PropertyNbrUnit<Long> begin,end;
        if (inEdge){
            begin = graphXFragment.getIEBegin(vertex);
            end = graphXFragment.getIEEnd(vertex);
        }
        else {
            begin = graphXFragment.geOEBegin(vertex);
            end = graphXFragment.getOEEnd(vertex);
        }

        Vertex<Long> nbrVertex = FFITypeFactoryhelper.newVertexLong();

        while (begin.getAddress() != end.getAddress()) {
            Long nbrVid = begin.vid();
            nbrVertex.SetValue(nbrVid);
            edgeTriplet.setDstOid(graphXFragment.getId(nbrVertex), newVdataArray.get(nbrVid));
            edgeTriplet.setAttr(newEdataArray.get(begin.eid()));
            Iterator<Tuple2<Long, MSG_T>> msgs = sendMsg.apply(edgeTriplet);
            messageStore.addMessages(msgs, graphXFragment, nextSet);
            begin.addV(16);
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

        /////////////////////////////////////Receive message////////////////////
        receiveMessage();

        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();

        if (curSet.cardinality() > 0) {
            logger.info("Before running round {}, frag [{}] has {} active vertices", round,
                graphXFragment.fid(), curSet.cardinality());
            vprogTime -= System.nanoTime();
            for (int lid = curSet.nextSetBit(0); lid >= 0 && lid < innerVerticesNum; lid = curSet.nextSetBit(lid + 1)) {
                vertex.SetValue((long) lid);
                Long oid = graphXFragment.getId(vertex);
                VD originalVD = newVdataArray.get(lid);
                newVdataArray.set(lid, vprog.apply(oid, originalVD, messageStore.get(lid)));
            }
            vprogTime += System.nanoTime();

            msgSendTime -= System.nanoTime();
            for (int lid = curSet.nextSetBit(0); lid >= 0; lid = curSet.nextSetBit(lid + 1)) {
                vertex.SetValue((long) lid);
                Long oid = graphXFragment.getId(vertex);
                edgeTriplet.setSrcOid(oid, newVdataArray.get(lid));
                iterateOnEdges(vertex, edgeTriplet);
            }
            msgSendTime += System.nanoTime();
            logger.info("[IncEval {}] Finish iterate edges for frag {}", round,
                graphXFragment.fid());
            flushTime -= System.nanoTime();
            try {
                messageStore.flushMessages(nextSet, messageManager, graphXFragment);
            } catch (IOException e) {
                e.printStackTrace();
            }
            nextSet.clear((int) innerVerticesNum, (int) verticesNum);
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

    public void postApp() {
        logger.info("Post app");
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
}
