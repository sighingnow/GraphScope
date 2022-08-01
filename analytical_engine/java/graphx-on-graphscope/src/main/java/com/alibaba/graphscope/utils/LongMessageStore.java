package com.alibaba.graphscope.utils;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.BaseGraphXFragment;
import com.alibaba.graphscope.graphx.graph.GSEdgeTripletImpl;
import com.alibaba.graphscope.graphx.utils.FixedBitSet;
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

public class LongMessageStore implements MessageStore<Long>{
    private Logger logger = LoggerFactory.getLogger(LongMessageStore.class.getName());

//    private long[] values;
    private AtomicLongArrayWrapper values;
    private Function2<Long,Long,Long> mergeMessage;
    private Vertex<Long>[] tmpVertex;
    private FFIByteVectorOutputStream[] outputStream;
    private FixedBitSet nextSet;

    public LongMessageStore(int len,int fnum, int numCores, Function2<Long,Long,Long> mergeMessage, FixedBitSet nextSet){
        values = new AtomicLongArrayWrapper(len);
        this.mergeMessage = mergeMessage;
        tmpVertex = new Vertex[numCores];
        for (int i = 0; i < numCores; ++i) {
            tmpVertex[i] = FFITypeFactoryhelper.newVertexLong();
        }
        outputStream = new FFIByteVectorOutputStream[fnum];
        for (int i = 0; i < fnum; ++i){
            outputStream[i] = new FFIByteVectorOutputStream();
        }
        this.nextSet = nextSet;
    }

    @Override
    public Long get(int index) {
        return values.get(index);
    }

    @Override
    public void set(int index, Long value) {
        values.compareAndSet(index,value);
    }

    @Override
    public int size() {
        return values.getSize();
    }

    @Override
    public void addMessages(
        Iterator<Tuple2<Long, Long>> msgs, BaseGraphXFragment<Long,Long,?,?> fragment,  int threadId,
        GSEdgeTripletImpl edgeTriplet,int srcLid, int dstLid) {
        while (msgs.hasNext()) {
            Tuple2<Long, Long> msg = msgs.next();
            long oid = msg._1();
            int lid;
            if (oid == edgeTriplet.dstId()){
                lid = dstLid;
            }
            else {
                lid = srcLid;
            }
            if (nextSet.get(lid)){
                long original = values.get(lid);
                values.compareAndSet(lid, mergeMessage.apply(original, msg._2()));
            }
            else {
                values.compareAndSet(lid,msg._2());
                nextSet.set(lid);
            }
        }
    }
    @Override
    public void flushMessages(FixedBitSet nextSet, DefaultMessageManager messageManager,
        BaseGraphXFragment<Long, Long, ?, ?> fragment,int [] fid2WorkerId) throws IOException {
        int ivnum = (int) fragment.getInnerVerticesNum();
        int cnt = 0;
        Vertex<Long> vertex = tmpVertex[0];

        for (int i = nextSet.nextSetBit(ivnum); i >= 0; i = nextSet.nextSetBit(i + 1)) {
            vertex.SetValue((long) i);
            int dstFid = fragment.getFragId(vertex);
            outputStream[dstFid].writeLong(fragment.getOuterVertexGid(vertex));
            outputStream[dstFid].writeLong(values.get(i));
            cnt += 1;
        }
        logger.debug("Frag [{}] try to send {} msg to outer vertices", fragment.fid(), cnt);
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
    public void digest(FFIByteVector vector, BaseGraphXFragment<Long, Long, ?, ?> fragment, FixedBitSet curSet) {
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
                long msg = inputStream.readLong();
                fragment.innerVertexGid2Vertex(gid, vertex);
                int lid = vertex.GetValue().intValue();
                if (curSet.get(lid)){
                    values.set(lid, mergeMessage.apply(values.get(lid), msg));
                }
                else {
                    //no update in curSet when the message store is not changed, although we receive vertices.
                    if (values.get(lid) != msg){
                        values.set(lid, msg);
                        curSet.set(lid);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
