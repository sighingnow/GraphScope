package com.alibaba.graphscope.mm.impl;

import com.alibaba.graphscope.conf.GraphXConf;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graph.GraphXVertexIdManager;
import com.alibaba.graphscope.graph.VertexDataManager;
import com.alibaba.graphscope.mm.MessageStore;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import java.util.BitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Function2;
import scala.Tuple2;

/**
 * Can store out messages or in messages.
 *
 * @param <MSG_T> message type
 */
public class DefaultMessageStore<MSG_T, VD> implements MessageStore<MSG_T, VD> {

    private Logger logger = LoggerFactory.getLogger(DefaultMessageStore.class.getName());

    private GraphXConf conf;
    private MSG_T[] values;
    private BitSet flags;
    private Function2<MSG_T, MSG_T, MSG_T> mergeMsg;
    private Function2<MSG_T,MSG_T,MSG_T> mergeMsgWithNull; //wrapper for user provided mergeMsg.
    private IFragment<?, ?, ?, ?> fragment;
    private int verticesNum, innerVerticesNum;
    private Vertex<Long> vertex;
    private GraphXVertexIdManager vertexIdManager;
    private VertexDataManager<VD> vertexDataManager;

    public DefaultMessageStore(GraphXConf conf) {
        this.conf = conf;
    }

    @Override
    public void init(IFragment<Long, Long, ?, ?> fragment, GraphXVertexIdManager idManager,
        VertexDataManager<VD> vertexDataManager,
        Function2<MSG_T, MSG_T, MSG_T> mergeMsg) {
        this.mergeMsg = mergeMsg;
        this.mergeMsgWithNull = (v1, v2) -> {
            if (v1 == null){
                return v2;
            }
            return mergeMsg.apply(v1, v2);
        };
        this.fragment = fragment;
        this.vertexIdManager = idManager;
        this.vertexDataManager = vertexDataManager;

        this.verticesNum = Math.toIntExact(fragment.getVerticesNum());
        this.innerVerticesNum = (int) fragment.getInnerVerticesNum();
        values = (MSG_T[]) new Object[verticesNum];
        flags = new BitSet(verticesNum);
        vertex = FFITypeFactoryhelper.newVertexLong();
    }

    public BitSet getFlags(){
        return flags;
    }

    @Override
    public boolean messageAvailable(long lid) {
        return flags.get((int) lid);
    }

    @Override
    public boolean hasMessages() {
        return !flags.isEmpty();
    }

    @Override
    public MSG_T getMessage(long lid) {
        return values[(int) lid];
    }

    @Override
    public void addLidMessage(long lid, MSG_T msg) {
        int intLid = (int) lid;
//        flags.set(intLid);
//        values[intLid] = mergeMsgWithNull.apply(values[intLid], msg);

        if (flags.get(intLid)) {
            values[intLid] = mergeMsg.apply(values[intLid], msg);
        } else {
            flags.set(intLid);
            values[intLid] = msg;
        }
    }

    @Override
    public void addOidMessage(long oid, MSG_T msg) {
//        logger.info("worker[{}] send msg to oid {}", fragment.fid(), oid);
        long lid = Math.toIntExact(vertexIdManager.oid2Lid(oid));
        addLidMessage(lid, msg);
    }

    @Override
    public void clear() {
        flags.clear();
    }

    @Override
    public void swap(MessageStore<MSG_T, VD> messageStore) {
        if (messageStore instanceof DefaultMessageStore) {
            DefaultMessageStore<MSG_T, VD> other = (DefaultMessageStore<MSG_T, VD>) messageStore;
            //only swap flags and values are ok
            logger.info("frag {} Before message store swap {} vs {}", fragment.fid(),
                this.flags.cardinality(),
                other.flags.cardinality());
            BitSet tmp = other.flags;
            other.flags = this.flags;
            this.flags = tmp;
            logger.info("frag {} After message store swap {} vs {}", fragment.fid(),
                this.flags.cardinality(),
                other.flags.cardinality());

            MSG_T[] tmpValues = other.values;
            other.values = this.values;
            this.values = tmpValues;
        }
    }

    @Override
    public void flushMessage(DefaultMessageManager messageManager) {
        int index = flags.nextSetBit(innerVerticesNum);
//        DoubleMsg msg = DoubleMsg.factory.create();
        int msgCnt = 0;
        while (index >= innerVerticesNum && index < verticesNum && index >= 0) {
            if (index == Integer.MAX_VALUE) {
                throw new IllegalStateException("Overflow is not expected");
            }
            vertex.SetValue((long) index);
            messageManager.syncStateOnOuterVertexArrowProjected(
                (ArrowProjectedFragment<Long, Long, ?, ?>) fragment.getFFIPointer(),
                vertex, values[index]);
            //CAUTION-------------------------------------------------------------
            //update outer vertices data here, otherwise will cause infinite message sending
            vertexDataManager.setVertexData(index, (VD) values[index]);
            index = flags.nextSetBit(index + 1);
            msgCnt += 1;
        }
        flags.clear(innerVerticesNum, verticesNum);
        logger.info("frag [{}] send msg of size {}", fragment.fid(), msgCnt);
    }
}
