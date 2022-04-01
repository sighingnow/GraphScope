package com.alibaba.graphscope.factory;

import com.alibaba.graphscope.communication.Communicator;
import com.alibaba.graphscope.conf.GraphXConf;
import com.alibaba.graphscope.graph.EdgeContextImpl;
import com.alibaba.graphscope.graph.GraphXVertexIdManager;
import com.alibaba.graphscope.graph.GraphxEdgeManager;
import com.alibaba.graphscope.graph.VertexDataManager;
import com.alibaba.graphscope.graph.VertexIdManager;
import com.alibaba.graphscope.graph.impl.GraphXVertexIdManagerImpl;
import com.alibaba.graphscope.graph.impl.GraphxEdgeManagerImpl;
import com.alibaba.graphscope.graph.impl.VertexDataManagerImpl;
import com.alibaba.graphscope.mm.MessageStore;
import com.alibaba.graphscope.mm.impl.DefaultMessageStore;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.utils.GraphXProxy;

/**
 * Providing factory methods for creating adaptor and adptor context.
 */
public class GraphXFactory {

    public static GraphXProxy createGraphXProxy(GraphXConf conf, DefaultMessageManager mm,
        Communicator communicator) {
        return new GraphXProxy(conf, mm, communicator);
    }

    public static GraphXVertexIdManager createIdManager(GraphXConf conf) {
        return new GraphXVertexIdManagerImpl(conf);
    }

    public static <VD> VertexDataManager createVertexDataManager(GraphXConf conf) {
        return new VertexDataManagerImpl(conf);
    }

    public static <MSG_T> MessageStore<MSG_T> createMessageStore(GraphXConf conf) {
        return new DefaultMessageStore<>(conf);
    }

    public static <VD, ED, MSG_T> EdgeContextImpl<VD, ED, MSG_T> createEdgeContext(
        GraphXConf conf) {
        return new EdgeContextImpl<>(conf);
    }

    public static <VD, ED, MSG_T> GraphxEdgeManager<VD, ED, MSG_T> createEdgeManager(
        GraphXConf conf, VertexIdManager<Long, Long> idManager, VertexDataManager<VD> vertexDataManager) {
        return new GraphxEdgeManagerImpl<>(conf, idManager, vertexDataManager);
    }

}
