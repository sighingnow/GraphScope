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
import com.alibaba.graphscope.graphx.GSEdgeTriplet;
import com.alibaba.graphscope.graphx.SerializationUtils;
import com.alibaba.graphscope.mm.MessageStore;
import com.alibaba.graphscope.mm.impl.DefaultMessageStore;
import com.alibaba.graphscope.mm.impl.ParallelMessageStore;
import com.alibaba.graphscope.utils.GraphXProxy;
import org.apache.spark.graphx.EdgeTriplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Function2;
import scala.Function3;
import scala.Tuple2;
import scala.collection.Iterator;

/**
 * Providing factory methods for creating adaptor and adptor context.
 */
public class GraphXFactory {

    private static Logger logger = LoggerFactory.getLogger(GraphXFactory.class.getName());

    public static <VD, ED, MSG> GraphXConf<VD, ED, MSG> createGraphXConf(
        Class<? extends VD> vdClass, Class<? extends ED> edClass, Class<? extends MSG> msgClass) {
        return new GraphXConf<VD, ED, MSG>(vdClass, edClass, msgClass);
    }

    private static <VD, ED, MSG> GraphXProxy<VD, ED, MSG> createGraphXProxy(
        GraphXConf<VD, ED, MSG> conf, Function3<Long, VD, MSG, VD> vprog,
        Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG>>> sendMsg,
        Function2<MSG, MSG, MSG> mergeMsg, int numCores) {
        return new GraphXProxy<VD, ED, MSG>(conf, vprog, sendMsg, mergeMsg, numCores);
    }

    public static <VD, ED, MSG> GraphXProxy<VD, ED, MSG> createGraphXProxy(
        GraphXConf<VD, ED, MSG> conf, String vprogFilePath, String sendMsgFilePath,
        String mergeMsgFilePath, int numCores) {
        Function3<Long, VD, MSG, VD> vprog = deserializeVprog(vprogFilePath, conf);
        Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG>>> sendMsg = deserializeSendMsg(
            sendMsgFilePath, conf);
        Function2<MSG, MSG, MSG> mergeMsg = deserializeMergeMsg(mergeMsgFilePath, conf);
        logger.info("deserialization success: {}, {}, {}", vprog, sendMsg, mergeMsg);

        GraphXProxy<VD, ED, MSG> graphXProxy = createGraphXProxy(conf, vprog, sendMsg, mergeMsg, numCores);
        logger.info("Construct graphx proxy: " + graphXProxy);
        return graphXProxy;
    }

    public static GraphXVertexIdManager createIdManager(GraphXConf conf) {
        return new GraphXVertexIdManagerImpl(conf);
    }

    public static <VD,ED,MSG> VertexDataManager createVertexDataManager(GraphXConf<VD,ED,MSG> conf) {
        return new VertexDataManagerImpl<VD>(conf);
    }

    public static <VD,ED,MSG> MessageStore<MSG,VD> createDefaultMessageStore(GraphXConf<VD,ED,MSG> conf) {
        return new DefaultMessageStore<MSG,VD>(conf);
    }
    public static <VD,ED,MSG> MessageStore<MSG,VD> createParallelMessageStore(GraphXConf<VD,ED,MSG> conf) {
        return new ParallelMessageStore<MSG,VD>(conf);
    }

    public static <VD, ED, MSG_T> EdgeContextImpl<VD, ED, MSG_T> createEdgeContext(
        GraphXConf<VD,ED,MSG_T> conf) {
        return new EdgeContextImpl<>(conf);
    }

    public static <VD, ED, MSG_T> GraphxEdgeManager<VD, ED, MSG_T> createEdgeManager(
        GraphXConf<VD,ED,MSG_T> conf, VertexIdManager<Long, Long> idManager,
        VertexDataManager<VD> vertexDataManager) {
        return new GraphxEdgeManagerImpl<>(conf, idManager, vertexDataManager);
    }

    public static <VD,ED>GSEdgeTriplet<VD,ED> createEdgeTriplet(GraphXConf<VD,ED,?> conf){
        return new GSEdgeTriplet<>();
    }

    private static <VD, ED, MSG> Function3<Long, VD, MSG, VD> deserializeVprog(String vprogFilePath,
        GraphXConf<VD, ED, MSG> conf) {
        try {
            Function3<Long, VD, MSG, VD> res = (Function3<Long, VD, MSG, VD>) SerializationUtils.read(
                vprogFilePath);
            return res;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new IllegalStateException("deserialization vprog failed");
        }
    }

    private static <VD, ED, MSG> Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG>>> deserializeSendMsg(
        String sendMsgFilePath, GraphXConf<VD, ED, MSG> conf) {
        try {
            Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG>>> res = (Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG>>>) SerializationUtils.read(
                sendMsgFilePath);
            return res;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new IllegalStateException("deserialization send msg failed");
        }
    }

    private static <VD, ED, MSG> Function2<MSG, MSG, MSG> deserializeMergeMsg(
        String mergeMsgFilePath, GraphXConf<VD, ED, MSG> conf) {
        try {
            Function2<MSG, MSG, MSG> res = (Function2<MSG, MSG, MSG>) SerializationUtils.read(
                mergeMsgFilePath);
            return res;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new IllegalStateException("deserialization merge msg failed");
        }
    }
}
