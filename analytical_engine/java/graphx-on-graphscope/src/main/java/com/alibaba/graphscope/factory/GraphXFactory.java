package com.alibaba.graphscope.factory;

import com.alibaba.graphscope.conf.GraphXConf;
import com.alibaba.graphscope.graph.EdgeContextImpl;
import com.alibaba.graphscope.graph.EdgeManager;
import com.alibaba.graphscope.graph.impl.EdgeManagerImpl;
import com.alibaba.graphscope.mm.MessageStore;
import com.alibaba.graphscope.mm.impl.DefaultMessageStore;
import com.alibaba.graphscope.utils.GraphXProxy;

/**
 * Providing factory methods for creating adaptor and adptor context.
 */
public class GraphXFactory {
    public static GraphXProxy createGraphXProxy(GraphXConf conf){
        return new GraphXProxy(conf);
    }

    public static <MSG_T>  MessageStore<MSG_T> createMessageStore(GraphXConf conf, long ivnum){
        return new DefaultMessageStore<>(conf, ivnum);
    }

    public static <VD,ED,MSG_T> EdgeContextImpl<VD,ED,MSG_T> createEdgeContext(GraphXConf conf, MessageStore<MSG_T> outMessageStore){
        return new EdgeContextImpl<>(conf, outMessageStore);
    }

    public static <ED>EdgeManager<ED> createEdgeManager(GraphXConf conf){
        return new EdgeManagerImpl<>(conf);
    }

}
