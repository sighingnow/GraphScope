package com.alibaba.graphscope.mm;


import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graph.GraphXVertexIdManager;
import com.alibaba.graphscope.graph.VertexDataManager;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import java.util.BitSet;
import scala.Function2;

public interface MessageStore<MSG_T,VD> {
    void init(IFragment<Long,Long,?,?> fragment, GraphXVertexIdManager idManager, VertexDataManager<VD> vertexDataManager, Function2<MSG_T,MSG_T,MSG_T> mergeMessage);

    boolean messageAvailable(long lid);

    boolean hasMessages();

    MSG_T getMessage(long lid);

    void addLidMessage(long lid, MSG_T msg);

    void addOidMessage(long oid, MSG_T msg);

    void clear();

    void swap(MessageStore<MSG_T,VD> messageStore);

    void flushMessage(DefaultMessageManager messageManager);

    BitSet getFlags();
}
