package com.alibaba.graphscope.utils;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.BaseGraphXFragment;
import com.alibaba.graphscope.graphx.graph.GSEdgeTripletImpl;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import java.io.IOException;
import java.util.BitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function2;
import scala.Tuple2;
import scala.collection.Iterator;

public class IntMessageStore implements MessageStore<Integer> {

    private Logger logger = LoggerFactory.getLogger(IntMessageStore.class.getName());

    private int[] values;
    private Function2<Integer, Integer, Integer> mergeMessage;
    private Vertex<Long>[] tmpVertex;
    private FFIByteVectorOutputStream[] outputStream;
    private BitSet nextSet;

    public IntMessageStore(int len, int fnum, int numCores, Function2<Integer, Integer, Integer> function2, BitSet nextSet) {
        values = new int[len];
        mergeMessage = function2;
        tmpVertex = new Vertex[numCores];
        for (int i = 0; i < numCores; ++i) {
            tmpVertex[i] = FFITypeFactoryhelper.newVertexLong();
        }
        outputStream = new FFIByteVectorOutputStream[fnum];
        for (int i = 0; i < fnum; ++i) {
            outputStream[i] = new FFIByteVectorOutputStream();
        }
        this.nextSet = nextSet;
    }

    @Override
    public Integer get(int index) {
        return values[index];
    }

    @Override
    public void set(int index, Integer value) {
        values[index] = value;
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public void addMessages(Iterator<Tuple2<Long, Integer>> msgs,
        BaseGraphXFragment<Long, Long, ?, ?> fragment, int threadId, GSEdgeTripletImpl edgeTriplet) {
        Vertex<Long> vertex = tmpVertex[threadId];
        while (msgs.hasNext()) {
            Tuple2<Long, Integer> msg = msgs.next();
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
            if (nextSet.get(lid)) {
                int original = values[lid];
                values[lid] = mergeMessage.apply(original, msg._2());
            } else {
                values[lid] = msg._2();
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
            outputStream[dstFid].writeInt(values[i]);
            cnt += 1;
        }
        logger.debug("Frag [{}] try to send {} msg to outer vertices", fragment.fid(), cnt);
        //finish stream
        for (int i = 0; i < fragment.fnum(); ++i) {
            if (i != fragment.fid()) {
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
    public void digest(FFIByteVector vector, BaseGraphXFragment<Long, Long, ?, ?> fragment, BitSet curSet) {
        FFIByteVectorInputStream inputStream = new FFIByteVectorInputStream(vector);
        int size = (int) vector.size();
        if (size <= 0) {
            throw new IllegalStateException("The received vector can not be empty");
        }
        Vertex<Long> vertex = tmpVertex[0];

//        logger.debug("IntMessageStore digest FFIVector size {}", size);
        try {
            while (inputStream.available() > 0) {
                long gid = inputStream.readLong();
                int msg = inputStream.readInt();
                fragment.innerVertexGid2Vertex(gid, vertex);
                int lid = vertex.GetValue().intValue();
                if (curSet.get(lid)){
                    values[lid] = mergeMessage.apply(values[lid], msg);
                }
                else {
                    //no update in curSet when the message store is not changed, although we receive vertices.
                    if (values[lid] != msg){
                        values[lid] = msg;
                        curSet.set(lid);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
