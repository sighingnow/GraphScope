package com.alibaba.graphscope.utils;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.BaseGraphXFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.BitSet;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.P;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;
import scala.Function2;
import scala.Tuple2;

public class ObjectMessageStore<T> implements MessageStore<T> {
    private Logger logger = LoggerFactory.getLogger(ObjectMessageStore.class.getName());

    private T [] values;
    private Class<? extends T> clz;
    private Function2<T,T,T> mergeMessage;
    private Vertex<Long> tmpVertex;
    private FFIByteVectorOutputStream[] ffiOutStream;
    private ObjectOutputStream[] outputStream;

    public ObjectMessageStore(int len,int fnum, Class<? extends T>clz, Function2<T,T,T> function2)
    {
        this.clz = clz;
        values = (T[]) new Object[len];
        mergeMessage = function2;
        tmpVertex = FFITypeFactoryhelper.newVertexLong();
        outputStream = new ObjectOutputStream[fnum];
        ffiOutStream = new FFIByteVectorOutputStream[fnum];
        for (int i = 0; i < fnum; ++i){
            ffiOutStream[i] = new FFIByteVectorOutputStream();
            try {
                outputStream[i] = new ObjectOutputStream(ffiOutStream[i]);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public T get(int index) {
        return values[index];
    }

    @Override
    public void set(int index, T value) {
        values[index] = value;
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public void addMessages(
        Iterator<Tuple2<Long, T>> msgs, BaseGraphXFragment<Long,Long,?,?> fragment, BitSet nextSet) {
        while (msgs.hasNext()) {
            Tuple2<Long, T> msg = msgs.next();
            if (!fragment.getVertex(msg._1(), tmpVertex)) {
                throw new IllegalStateException("get vertex for oid failed: " + msg._1());
            }
            int lid = tmpVertex.GetValue().intValue();
            if (nextSet.get(lid)){
                T original = values[lid];
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
        BaseGraphXFragment<Long, Long, ?, ?> fragment, int[] fid2WorkerId) throws IOException {
        int ivnum = (int) fragment.getInnerVerticesNum();
        int cnt = 0;

        for (int i = nextSet.nextSetBit(ivnum); i >= 0; i = nextSet.nextSetBit(i + 1)) {
            tmpVertex.SetValue((long) i);
            int dstFid = fragment.getFragId(tmpVertex);
            outputStream[dstFid].writeLong(fragment.getOuterVertexGid(tmpVertex));
            outputStream[dstFid].writeObject(values[i]);
            cnt += 1;
        }
        logger.info("Frag [{}] try to send {} msg to outer vertices", fragment.fid(), cnt);
        //finish stream
        for (int i = 0; i < fragment.fnum(); ++i){
            if (i != fragment.fid()){
                outputStream[i].flush();
                ffiOutStream[i].finishSetting();
                if (ffiOutStream[i].getVector().size() > 0){
                    int workerId = fid2WorkerId[i];
                    messageManager.sendToFragment(workerId, ffiOutStream[i].getVector());
                    logger.info("fragment [{}] send {} bytes to [{}]", fragment.fid(), ffiOutStream[i].getVector().size(), i);
                }
                ffiOutStream[i].reset();
                outputStream[i] = new ObjectOutputStream(ffiOutStream[i]);
            }
        }
    }

    @Override
    public void digest(FFIByteVector vector, BaseGraphXFragment<Long,Long,?,?> fragment, BitSet curSet) {
        ObjectInputStream inputStream = null;
        try {
            inputStream = new ObjectInputStream(new FFIByteVectorInputStream(vector));
        } catch (IOException e) {
            e.printStackTrace();
        }
        int size = (int) vector.size();
        if (size <= 0) {
            throw new IllegalStateException("The received vector can not be empty");
        }

        try {
            logger.info("DefaultMessageStore digest FFIVector size {}, available {}", size, inputStream.available());
            while (inputStream.available() > 0) {
                long gid = inputStream.readLong();
                T msg = (T) inputStream.readObject();
                fragment.innerVertexGid2Vertex(gid, tmpVertex);
                int lid = tmpVertex.GetValue().intValue();
                logger.info("Digesting message for lid {}, msg {} curSet status {}", tmpVertex.GetValue(), msg, curSet.get(lid));
                if (curSet.get(lid)){
                    values[lid] = mergeMessage.apply(values[lid], msg);
                }
                else {
                    //no update in curSet when the message store is not changed, although we receive vertices.
                    if (values[lid] == null){
                        values[lid] = msg;
                    }
                    else if (!values[lid].equals(msg)){
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
