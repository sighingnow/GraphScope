package com.alibaba.graphscope.utils;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.BaseGraphXFragment;
import com.alibaba.graphscope.graphx.graph.GSEdgeTripletImpl;
import com.alibaba.graphscope.graphx.utils.IdParser;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import java.io.IOException;
import java.util.BitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;
import scala.Function2;
import scala.Tuple2;

public class DoubleMessageStore implements MessageStore<Double> {
    private Logger logger = LoggerFactory.getLogger(DoubleMessageStore.class.getName());

//    private double[] values;
    private AtomicDoubleArrayWrapper values;
    private Function2<Double,Double,Double> mergeMessage;
    private Vertex<Long> tmpVertex[];
    private FFIByteVectorOutputStream[] outputStream;
    private IdParser idParser;

    public DoubleMessageStore(int len, int fnum,int numCores, Function2<Double,Double,Double> function2) {
        values = new AtomicDoubleArrayWrapper(len);
        mergeMessage = function2;
        tmpVertex = new Vertex[numCores];
        for (int i = 0; i < numCores; ++i){
            tmpVertex[i]= FFITypeFactoryhelper.newVertexLong();
        }
        outputStream = new FFIByteVectorOutputStream[fnum];
        for (int i = 0; i < fnum; ++i){
            outputStream[i] = new FFIByteVectorOutputStream();
        }
        idParser = new IdParser(fnum);
    }

    @Override
    public Double get(int index) {
        return values.get(index);
    }

    @Override
    public void set(int index, Double value) {
        values.set(index, value);
    }

    @Override
    public int size() {
        return values.getSize();
    }


    @Override
    public void addMessages(
        Iterator<Tuple2<Long, Double>> msgs, BaseGraphXFragment<Long,Long,?,?> fragment, BitSet nextSet, int threadId, GSEdgeTripletImpl edgeTriplet) {
        Vertex<Long> vertex = tmpVertex[threadId];
        while (msgs.hasNext()) {
            Tuple2<Long, Double> msg = msgs.next();
            //the oid must from src or dst, we first find with lid.
            long oid = msg._1();
            int lid;
            if (oid == edgeTriplet.srcId()){
                lid = (int) edgeTriplet.srcLid();
            }
            else if (oid == edgeTriplet.dstId()){
                lid = (int) edgeTriplet.dstLid();
            }
            else {
                if (!fragment.getVertex(msg._1(), vertex)) {
                    throw new IllegalStateException("get vertex for oid failed: " + msg._1());
                }
                lid = vertex.GetValue().intValue();
            }
            if (nextSet.get(lid)){
                double original = values.get(lid);
                values.compareAndSet(lid, mergeMessage.apply(original, msg._2()));
            }
            else {
                values.compareAndSet(lid, msg._2());
                nextSet.set(lid);
            }
        }
    }

    @Override
    public void flushMessages(BitSet nextSet, DefaultMessageManager messageManager,
        BaseGraphXFragment<Long, Long, ?, ?> fragment, int[] fid2WorkerId) throws IOException {
        int ivnum = (int) fragment.getInnerVerticesNum();
        int cnt = 0;
        Vertex<Long> vertex = tmpVertex[0];

        for (int i = nextSet.nextSetBit(ivnum); i >= 0; i = nextSet.nextSetBit(i + 1)) {
            vertex.SetValue((long) i);
            int dstFid = fragment.getFragId(vertex);
            outputStream[dstFid].writeLong(fragment.getOuterVertexGid(vertex));
            outputStream[dstFid].writeDouble(values.get(i));
            cnt += 1;
        }
        logger.info("Frag [{}] try to send {} msg to outer vertices", fragment.fid(), cnt);
        //finish stream
        for (int i = 0; i < fragment.fnum(); ++i){
            if (i != fragment.fid()){
                outputStream[i].finishSetting();
                if (outputStream[i].getVector().size() > 0){
                    int workerId = fid2WorkerId[i];
                    messageManager.sendToFragment(workerId, outputStream[i].getVector());
//                    logger.info("fragment [{}] send {} bytes to [{}]", fragment.fid(), outputStream[i].getVector().size(), i);
                }
                outputStream[i].reset();
            }
        }
    }

    @Override
    public void digest(FFIByteVector vector, BaseGraphXFragment<Long,Long,?,?> fragment, BitSet curSet) {
        FFIByteVectorInputStream inputStream = new FFIByteVectorInputStream(vector);
        int size = (int) vector.size();
        if (size <= 0) {
            throw new IllegalStateException("The received vector can not be empty");
        }
        Vertex<Long> vertex = tmpVertex[0];

//        logger.debug("DefaultMessageStore digest FFIVector size {}", size);
        try {
            while (inputStream.available() > 0) {
                long gid = inputStream.readLong();
                double msg = inputStream.readDouble();
                int fid = idParser.getFragId(gid);
                if (fid != fragment.fid()){
                    throw new IllegalStateException("receive a non inner vertex gid: "+ gid + ", parsed fid: " + fid + ", our fid: " + fragment.fid());
                }
                if (!fragment.innerVertexGid2Vertex(gid, vertex)){
                    throw new IllegalStateException("inner vertex gid 2 vertex failed");
                }
                int lid = vertex.GetValue().intValue();
                if (curSet.get(lid)){
                    if (lid < 5) {
                        logger.info("merge msg for lid {}, prev {}, received {}", lid,
                            values.get(lid), msg);
                    }
                    values.set(lid, mergeMessage.apply(values.get(lid), msg));
                }
                else {
                    //no update in curSet when the message store is not changed, although we receive vertices.
                    if (values.get(lid) != msg){
                        if (lid < 5) {
                            logger.info("merge msg for lid {}, prev {}, received {}", lid,
                                values.get(lid), msg);
                        }
                        values.set(lid, msg);
                        curSet.set(lid);
                    }
                    if (lid < 5) {
                        logger.info(
                            "skip seting message since the old value is same as input {}, old {} msg{}",
                            lid, values.get(lid), msg);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
