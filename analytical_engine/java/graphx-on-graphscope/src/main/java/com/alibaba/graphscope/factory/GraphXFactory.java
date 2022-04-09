package com.alibaba.graphscope.factory;

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
import com.alibaba.graphscope.utils.GraphXProxy;
import org.apache.spark.graphx.EdgeTriplet;
import scala.Function1;
import scala.Function2;
import scala.Function3;
import scala.Tuple2;
import scala.collection.Iterator;

/**
 * Providing factory methods for creating adaptor and adptor context.
 */
public class GraphXFactory {

    public static <VD, ED, MSG> GraphXConf<VD, ED, MSG> createGraphXConf(
        Class<? extends VD> vdClass, Class<? extends ED> edClass, Class<? extends MSG> msgClass) {
        return new GraphXConf<VD, ED, MSG>(vdClass, edClass, msgClass);
    }

    public static <VD, ED, MSG> GraphXProxy<VD, ED, MSG> createGraphXProxy(
        GraphXConf<VD, ED, MSG> conf, Function3<Long, VD, MSG, VD> vprog,
        Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG>>> sendMsg,
        Function2<MSG, MSG, MSG> mergeMsg) {
        return new GraphXProxy<VD, ED, MSG>(conf, vprog, sendMsg, mergeMsg);
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
        GraphXConf conf, VertexIdManager<Long, Long> idManager,
        VertexDataManager<VD> vertexDataManager) {
        return new GraphxEdgeManagerImpl<>(conf, idManager, vertexDataManager);
    }

}
