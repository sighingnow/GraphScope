package com.alibaba.graphscope.graph.impl;

import com.alibaba.graphscope.conf.GraphXConf;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graph.AbstractEdgeManager;
import com.alibaba.graphscope.graph.GrapeEdge;
import com.alibaba.graphscope.graph.GraphxEdgeManager;
import com.alibaba.graphscope.graph.VertexDataManager;
import com.alibaba.graphscope.graph.VertexIdManager;
import com.alibaba.graphscope.graphx.GSEdgeTriplet;
import com.alibaba.graphscope.mm.MessageStore;
import org.apache.spark.graphx.EdgeTriplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Tuple2;
import scala.collection.Iterator;

public class GraphxEdgeManagerImpl<VD, ED, MSG_T> extends
    AbstractEdgeManager<Long, Long, Long, ED, ED> implements
    GraphxEdgeManager<VD, ED, MSG_T> {

    private static Logger logger = LoggerFactory.getLogger(GraphxEdgeManagerImpl.class.getName());

    private GraphXConf conf;
    private VertexIdManager<Long, Long> idManager;
    private VertexDataManager<VD> vertexDataManager;
//    private long[] threadNumEdges;
//    private int[] threadNbrPos;
    private Long[] dstOids;
    private Long[] dstLids;
    private ED[] edatas;
    private int[] nbrPositions;
    private long[] numOfEdges;

    public GraphxEdgeManagerImpl(GraphXConf conf, VertexIdManager<Long, Long> idManager,
        VertexDataManager<VD> vertexDataManager) {
        this.conf = conf;
        this.idManager = idManager;
        this.vertexDataManager = vertexDataManager;
    }

    public TupleIterable getTupleIterable(int threadId){
        return edgeIterables.get(threadId);
    }

    @Override
    public void init(IFragment<Long, Long, VD, ED> fragment, int numCores) {
        super.init(fragment, idManager, Long.class, Long.class, conf.getEdataClass(), conf.getEdataClass(),
            null, numCores);
//        threadNumEdges = new long[numCores];
//        threadNbrPos = new int[numCores];
        dstOids = csrHolder.dstOids;
        dstLids = csrHolder.dstLids;
        edatas = csrHolder.edatas;
        nbrPositions = csrHolder.nbrPositions;
        numOfEdges = csrHolder.numOfEdges;
        logger.info("create EdgeManagerImpl({})", fragment.fid());
    }

    /**
     * Iterator over edges start from srcLid, update dstId info in context, and apply functions to
     * context;
     *
     * @param srcLid          src lid
     * @param msgSender       mapping from edge triplet to a iterator for (dstId, msg).
     * @param outMessageStore
     */
    @Override
    public void iterateOnEdges(long srcLid, GSEdgeTriplet<VD, ED> triplet,
        Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG_T>>> msgSender,
        MessageStore<MSG_T, VD> outMessageStore) {
        edgeIterable.setLid(srcLid);
        for (GrapeEdge<Long, Long, ED> edge : edgeIterable) {
            triplet.setDstOid(edge.dstOid, vertexDataManager.getVertexData(edge.dstLid), edge.value);
//            context.setDstValues(edge.dstOid, edge.dstLid, vertexDataManager.getVertexData(edge.dstLid), edge.value);
            //Avoid creating edge triplet.
            Iterator<Tuple2<Long, MSG_T>> iterator = msgSender.apply(triplet);
//            logger.info("Edge ctx: srcLid{}, srcOid {}, dstLid {}, dstOid {}, dstVdata {}, edge value{}", srcLid, context.srcId(), edge.dstLid, edge.dstOid, vertexDataManager.getVertexData(edge.dstLid), edge.value);
            while (iterator.hasNext()) {
                Tuple2<Long, MSG_T> tuple2 = iterator.next();
                //logger.info("cur tuple: {}", tuple2);
//                logger.info("src lid {}(oid {}) send {} to {} when visiting edge ({},{})",srcLid, idManager.lid2Oid(srcLid), tuple2._2(), tuple2._1(), edge.dstOid, edge.value);
                outMessageStore.addOidMessage(tuple2._1(), tuple2._2());
            }
        }
    }
    @Override
    public void iterateOnEdgesParallel(int threadId, long srcLid, GSEdgeTriplet<VD, ED> triplet,
        Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG_T>>> msgSender,
        MessageStore<MSG_T, VD> outMessageStore) {
        long numEdge = numOfEdges[(int) srcLid];
        int nbrPos = nbrPositions[(int) srcLid];
        for (int i = nbrPos; i < nbrPos + numEdge; ++i){
            triplet.setDstOid(dstOids[i], vertexDataManager.getVertexData(dstLids[i]), edatas[i]);
            Iterator<Tuple2<Long, MSG_T>> iterator = msgSender.apply(triplet);
            while (iterator.hasNext()) {
                Tuple2<Long, MSG_T> tuple2 = iterator.next();
                outMessageStore.addOidMessage(tuple2._1(), tuple2._2());
            }
        }

//        TupleIterable iterable = edgeIterables.get(threadId);
//
//        iterable.setLid(srcLid);
//        for (GrapeEdge<Long, Long, ED> edge : iterable) {
//            triplet.setDstOid(edge.dstOid, vertexDataManager.getVertexData(edge.dstLid), edge.value);
//            Iterator<Tuple2<Long, MSG_T>> iterator = msgSender.apply(triplet);
//            while (iterator.hasNext()) {
//                Tuple2<Long, MSG_T> tuple2 = iterator.next();
//                outMessageStore.addOidMessage(tuple2._1(), tuple2._2());
//            }
//        }
    }

}
