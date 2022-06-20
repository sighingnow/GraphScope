package com.alibaba.graphscope.utils;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.BaseGraphXFragment;
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

    private long[] values;
    private Function2<Long,Long,Long> mergeMessage;
    private Vertex<Long> tmpVertex;
    private FFIByteVectorOutputStream[] outputStream;

    public LongMessageStore(int len,int fnum, Function2<Long,Long,Long> mergeMessage){
        values = new long[len];
        this.mergeMessage = mergeMessage;
        tmpVertex = FFITypeFactoryhelper.newVertexLong();
        outputStream = new FFIByteVectorOutputStream[fnum];
        for (int i = 0; i < fnum; ++i){
            outputStream[i] = new FFIByteVectorOutputStream();
        }
    }

    @Override
    public Long get(int index) {
        return values[index];
    }

    @Override
    public void set(int index, Long value) {
        values[index] =value;
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public void addMessages(
        Iterator<Tuple2<Long, Long>> msgs, BaseGraphXFragment<Long,Long,?,?> fragment, BitSet nextSet) {
        while (msgs.hasNext()) {
            Tuple2<Long, Long> msg = msgs.next();
            if (!fragment.getVertex(msg._1(), tmpVertex)) {
                throw new IllegalStateException("get vertex for oid failed: " + msg._1());
            }
            int lid = tmpVertex.GetValue().intValue();
            if (nextSet.get(lid)){
                long original = values[lid];
                values[lid] = mergeMessage.apply(original, msg._2());
            }
            else {
                values[lid] = msg._2();
                nextSet.set(lid);
            }
        }
    }
    @Override
    public void flushMessages(BitSet nextSet, DefaultMessageManager messageManager,
        BaseGraphXFragment<Long, Long, ?, ?> fragment) throws IOException {
        int ivnum = (int) fragment.getInnerVerticesNum();
        int cnt = 0;

        for (int i = nextSet.nextSetBit(ivnum); i >= 0; i = nextSet.nextSetBit(i + 1)) {
            tmpVertex.SetValue((long) i);
            int dstFid = fragment.getFragId(tmpVertex);
            outputStream[dstFid].writeLong(fragment.getOuterVertexGid(tmpVertex));
            outputStream[dstFid].writeLong(values[i]);
            cnt += 1;
        }
        logger.info("Frag [{}] try to send {} msg to outer vertices", fragment.fid(), cnt);
        //finish stream
        for (int i = 0; i < fragment.fnum(); ++i){
            if (i != fragment.fid()){
                outputStream[i].finishSetting();
                messageManager.sendToFragment(i, outputStream[i].getVector());
                logger.info("fragment [{}] send {} bytes to [{}]", fragment.fid(), outputStream[i].getVector().size(), i);
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

        logger.debug("IntMessageStore digest FFIVector size {}", size);
        try {
            while (inputStream.available() > 0) {
                long gid = inputStream.readLong();
                long msg = inputStream.readLong();
                fragment.innerVertexGid2Vertex(gid, tmpVertex);
                int lid = tmpVertex.GetValue().intValue();
                if (curSet.get(lid)){
                    values[lid] = mergeMessage.apply(values[lid], msg);
                }
                else {
                    values[lid] = msg;
                    curSet.set(lid);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
