package com.alibaba.graphscope.mm.impl;

import com.alibaba.graphscope.conf.GraphXConf;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graph.GraphXVertexIdManager;
import com.alibaba.graphscope.mm.MessageStore;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import java.util.BitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function2;

/**
 * Can store out messages or in messages.
 * @param <MSG_T> message type
 */
public class DefaultMessageStore<MSG_T> implements MessageStore<MSG_T> {
    private Logger logger = LoggerFactory.getLogger(DefaultMessageStore.class.getName());

    private GraphXConf conf;
    private MSG_T[] values;
    private BitSet flags;
    private Function2<MSG_T, MSG_T, MSG_T> mergeMsg;
    private IFragment<?,?,?,?> fragment;
    private int verticesNum, innerVerticesNum;
    private Vertex<Long> vertex;
    private GraphXVertexIdManager vertexIdManager;

    public DefaultMessageStore(GraphXConf conf) {
        this.conf = conf;
    }

    @Override
    public void init(IFragment<Long,Long,?,?> fragment, GraphXVertexIdManager idManager, Function2<MSG_T, MSG_T, MSG_T> mergeMsg) {
        this.mergeMsg = mergeMsg;
        this.fragment = fragment;
        this.vertexIdManager = idManager;

        this.verticesNum = Math.toIntExact(fragment.getVerticesNum());
        this.innerVerticesNum = (int) fragment.getInnerVerticesNum();
        values = (MSG_T[]) new Object[verticesNum];
        flags = new BitSet(verticesNum);
        vertex = FFITypeFactoryhelper.newVertexLong();
    }

    @Override
    public boolean messageAvailable(long lid) {
        return flags.get((int)lid);
    }

    @Override
    public MSG_T getMessage(long lid) {
        return values[(int) lid];
    }

    @Override
    public void addLidMessage(long lid, MSG_T msg) {
        flags.set((int) lid);
        values[(int) lid] = msg;
    }

    @Override
    public void addOidMessage(long oid, MSG_T msg) {
        long lid = Math.toIntExact(vertexIdManager.oid2Lid(oid));
        flags.set((int)lid);
        values[(int)lid] = msg;
    }

    @Override
    public void clear() {
        flags.clear();
    }

    @Override
    public void swap(MessageStore<MSG_T> messageStore) {
        if (messageStore instanceof DefaultMessageStore){
            DefaultMessageStore<MSG_T> other = (DefaultMessageStore<MSG_T>) messageStore;
            //only swap flags and values are ok
            logger.info("Before message store swap {} vs {}", this.flags.cardinality(), other.flags.cardinality());
            BitSet tmp = other.flags;
            other.flags = this.flags;
            this.flags = tmp;
            logger.info("After message store swap {} vs {}", this.flags.cardinality(), other.flags.cardinality());

            MSG_T[] tmpValues = other.values;
            other.values = this.values;
            this.values = tmpValues;
        }
    }

    @Override
    public void flushMessage(DefaultMessageManager messageManager) {
        int index = flags.nextSetBit(innerVerticesNum);
        while (index >= innerVerticesNum && index < verticesNum){
            index = flags.nextSetBit(index);
            vertex.SetValue((long) index);
            messageManager.syncStateOnOuterVertex(fragment, vertex, values[index]);
            logger.info("frag [{}] Sync state on out vertices {}, msg {}", fragment.fid(), vertex.GetValue(), values[index]);
            flags.clear(index);
        }
    }
}
