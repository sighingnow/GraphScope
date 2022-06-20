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

public class DoubleMessageStore implements MessageStore<Double> {
    private Logger logger = LoggerFactory.getLogger(DoubleMessageStore.class.getName());

    private double[] values;
    private Function2<Double,Double,Double> mergeMessage;
    private Vertex<Long> tmpVertex;
    private FFIByteVectorOutputStream[] outputStream;

    public DoubleMessageStore(int len, int fnum, Function2<Double,Double,Double> function2) {
        values = new double[len];
        mergeMessage = function2;
        tmpVertex = FFITypeFactoryhelper.newVertexLong();
        outputStream = new FFIByteVectorOutputStream[fnum];
        for (int i = 0; i < fnum; ++i){
            outputStream[i] = new FFIByteVectorOutputStream();
        }
    }



    @Override
    public Double get(int index) {
        return values[index];
    }

    @Override
    public void set(int index, Double value) {
        values[index] = value;
    }

    @Override
    public int size() {
        return values.length;
    }


    @Override
    public void addMessages(
        Iterator<Tuple2<Long, Double>> msgs, BaseGraphXFragment<Long,Long,?,?> fragment, BitSet nextSet) {
        while (msgs.hasNext()) {
            Tuple2<Long, Double> msg = msgs.next();
            if (!fragment.getVertex(msg._1(), tmpVertex)) {
                throw new IllegalStateException("get vertex for oid failed: " + msg._1());
            }
            int lid = tmpVertex.GetValue().intValue();
            if (nextSet.get(lid)){
                double original = values[lid];
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
            outputStream[dstFid].writeDouble(values[i]);
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
    public void digest(FFIByteVector vector, BaseGraphXFragment<Long,Long,?,?> fragment, BitSet curSet) {
        FFIByteVectorInputStream inputStream = new FFIByteVectorInputStream(vector);
        int size = (int) vector.size();
        if (size <= 0) {
            throw new IllegalStateException("The received vector can not be empty");
        }

        logger.debug("DefaultMessageStore digest FFIVector size {}", size);
        try {
            while (inputStream.available() > 0) {
                long gid = inputStream.readLong();
                double msg = inputStream.readDouble();
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
