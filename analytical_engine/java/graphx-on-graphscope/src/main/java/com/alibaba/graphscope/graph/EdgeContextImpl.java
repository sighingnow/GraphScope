package com.alibaba.graphscope.graph;

import com.alibaba.graphscope.conf.GraphXConf;
import com.alibaba.graphscope.mm.MessageStore;
import org.apache.spark.graphx.EdgeContext;

public class EdgeContextImpl<VD,ED,MSG_T> extends EdgeContext<VD,ED,MSG_T> {
    private GraphXConf conf;
    private long srcId, dstId;
    private long localSrcId, localDstId;
    private VD srcAttr,dstAttr;
    private ED edgeAttr;
    private MessageStore<MSG_T,VD> outgoingMessageStore;
    public EdgeContextImpl(GraphXConf conf){
        this.conf = conf;
        srcId = dstId = localSrcId = localDstId = -1;
        edgeAttr = null;
        srcAttr = dstAttr = null;
    }

    public void init(MessageStore<MSG_T,VD> messageStore){
        this.outgoingMessageStore = messageStore;
    }


    public void setAllValues(long srcId, long localSrcId, long dstId, long localDstId, VD srcAttr, VD dstAttr, ED edgeAttr){
        this.srcId = srcId;
        this.dstId = dstId;
        this.srcAttr = srcAttr;
        this.dstAttr = dstAttr;
        this.edgeAttr = edgeAttr;
        this.localSrcId = localSrcId;
        this.localDstId = localDstId;
    }

    public void setSrcValues(long srcId, long localSrcId, VD srcAttr){
        this.srcId = srcId;
        this.localSrcId = localSrcId;
        this.srcAttr = srcAttr;
    }

    public void setDstValues(long dstId, long localDstIdId, VD dstAttr, ED edgeAttr){
        this.dstId = dstId;
        this.localDstId = localDstIdId;
        this.dstAttr = dstAttr;
        this.edgeAttr = edgeAttr;
    }

    @Override
    public long srcId() {
        return srcId;
    }

    @Override
    public long dstId() {
        return dstId;
    }

    @Override
    public VD srcAttr() {
        return srcAttr;
    }

    @Override
    public VD dstAttr() {
        return dstAttr;
    }

    @Override
    public ED attr() {
        return edgeAttr;
    }

    @Override
    public void sendToSrc(MSG_T msg) {
        this.outgoingMessageStore.addLidMessage(localSrcId, msg);
    }

    @Override
    public void sendToDst(MSG_T msg) {
        this.outgoingMessageStore.addLidMessage(localDstId, msg);
    }
}
