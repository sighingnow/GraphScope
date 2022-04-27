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
import java.util.List;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeTriplet;
import org.apache.spark.graphx.ReusableEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Tuple2;
import scala.collection.Iterator;

public class GraphxEdgeManagerImpl<VD, ED, MSG_T> extends
    AbstractEdgeManager<Long, Long, Long, ED, ED> implements
    GraphxEdgeManager<VD, ED, MSG_T> {

    private static Logger logger = LoggerFactory.getLogger(GraphxEdgeManagerImpl.class.getName());

    protected GraphXConf conf;
    protected VertexIdManager<Long, Long> idManager;
    protected VertexDataManager<VD> vertexDataManager;
    protected Long[] dstOids;
    protected Long[] dstLids;
    protected ED[] edatas;
    protected int[] nbrPositions;
    protected long[] numOfEdges;

    public GraphxEdgeManagerImpl(GraphXConf conf, VertexIdManager<Long, Long> idManager,
        VertexDataManager<VD> vertexDataManager) {
        this.conf = conf;
        this.idManager = idManager;
        this.vertexDataManager = vertexDataManager;
    }

    @Override
    public void init(IFragment<Long, Long, VD, ED> fragment, int numCores) {
        super.init(fragment, idManager, Long.class, Long.class, conf.getEdataClass(),
            conf.getEdataClass(),
            null, numCores);
        dstOids = csrHolder.dstOids;
        dstLids = csrHolder.dstLids;
        edatas = csrHolder.edatas;
        nbrPositions = csrHolder.nbrPositions;
        numOfEdges = csrHolder.numOfEdges;
        logger.info("create EdgeManagerImpl({})", fragment.fid());
    }

    @Override
    public synchronized Iterator<Edge<ED>> iterator(long startLid, long endLid) {
        return new Iterator<Edge<ED>>() {
            private long curLid = startLid;
            private ReusableEdge<ED> edge = new ReusableEdge<ED>();
            long numEdge = numOfEdges[(int) curLid];
            int nbrPos = nbrPositions[(int) curLid];
            int endPos = (int) (nbrPos + numEdge);
            int curPos = nbrPos;

            @Override
            public boolean hasNext() {
                //logger.info("has next: curLId {} endLid {} curPos {} endPos {} numEdge {}", curLid, endLid, curPos, endPos, numEdge);
                if (curLid >= endLid) {
                    return false;
                }
                if (curPos < endPos) {
                    return true;
                } else {
                    curLid += 1;
                    while (curLid < endLid) {
                        numEdge = numOfEdges[(int) curLid];
                        if (numEdge > 0) {
                            break;
                        }
                        curLid += 1;
                    }
                    if (curLid >= endLid) {
                        return false;
                    }
                    nbrPos = nbrPositions[(int) curLid];
                    endPos = (int) (nbrPos + numEdge);
                    curPos = nbrPos;
                    //logger.info("has next move to new lid: curLId {} endLid {} curPos {} endPos {} numEdge {}", curLid, endLid, curPos, endPos, numEdge);
                    edge.setSrcId(idManager.lid2Oid(curLid));
                    return true;
                }
            }

            @Override
            public Edge<ED> next() {
                edge.setDstId(dstOids[curPos]);
                edge.setAttr(edatas[curPos]);
                //	logger.info("src{}, dst{}}", dstOids[curPos], edatas[curPos]);
                curPos += 1;
                return edge;
            }
        };
    }

    @Override
    public long getPartialEdgeNum(long startLid, long endLid) {
        long startLidPos = nbrPositions[(int) startLid];
        long endLidPos = nbrPositions[(int) endLid];
        return numOfEdges[(int) endLid] + endLidPos - startLidPos;
    }

    @Override
    public long getTotalEdgeNum(){
        return dstLids.length;
    }

    /**
     * Iterator over edges start from srcLid, update dstId info in context, and apply functions to
     * context;
     *
     * @param srcLid    src lid
     * @param msgSender mapping from edge triplet to a iterator for (dstId, msg).
     */
    @Override
    public void iterateOnEdges(long srcLid, GSEdgeTriplet<VD, ED> triplet,
        Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG_T>>> msgSender,
        MessageStore<MSG_T, VD> outMessageCache) {
        edgeIterable.setLid(srcLid);
        for (GrapeEdge<Long, Long, ED> edge : edgeIterable) {
            triplet.setDstOid(edge.dstOid, vertexDataManager.getVertexData(edge.dstLid),
                edge.value);
//            context.setDstValues(edge.dstOid, edge.dstLid, vertexDataManager.getVertexData(edge.dstLid), edge.value);
            //Avoid creating edge triplet.
            Iterator<Tuple2<Long, MSG_T>> iterator = msgSender.apply(triplet);
//            logger.info("Edge ctx: srcLid{}, srcOid {}, dstLid {}, dstOid {}, dstVdata {}, edge value{}", srcLid, context.srcId(), edge.dstLid, edge.dstOid, vertexDataManager.getVertexData(edge.dstLid), edge.value);
            while (iterator.hasNext()) {
                Tuple2<Long, MSG_T> tuple2 = iterator.next();
                //logger.info("cur tuple: {}", tuple2);
//                logger.info("src lid {}(oid {}) send {} to {} when visiting edge ({},{})",srcLid, idManager.lid2Oid(srcLid), tuple2._2(), tuple2._1(), edge.dstOid, edge.value);
                outMessageCache.addOidMessage(tuple2._1(), tuple2._2());
            }
        }
    }

    @Override
    public void iterateOnEdgesParallel(int threadId, long srcLid, GSEdgeTriplet<VD, ED> triplet,
        Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG_T>>> msgSender,
        MessageStore<MSG_T, VD> outMessageCache) {
        long numEdge = numOfEdges[(int) srcLid];
        int nbrPos = nbrPositions[(int) srcLid];
        int endPos = (int) (nbrPos + numEdge);
        for (int i = nbrPos; i < endPos; ++i) {
            triplet.setDstOid(dstOids[i], vertexDataManager.getVertexData(dstLids[i]), edatas[i]);
            Iterator<Tuple2<Long, MSG_T>> iterator = msgSender.apply(triplet);
            logger.info("for edge: {}->{}", triplet.srcId(), triplet.dstId());
            while (iterator.hasNext()) {
                Tuple2<Long, MSG_T> tuple2 = iterator.next();
                outMessageCache.addOidMessage(tuple2._1(), tuple2._2());
            }
        }
    }

    @Override
    public <ED2> GraphxEdgeManager<VD, ED2, MSG_T> withNewEdgeData(List<ED2> newEdgeData, long startLid, long endLid) {
        if (newEdgeData.size() != getPartialEdgeNum(startLid, endLid)){
            throw new IllegalStateException("Unmatched size: " + newEdgeData.size() + " parial edge num: " + getPartialEdgeNum(startLid,endLid));
        }
        return new GraphxEdgeManagerUpdateEData<VD,ED2,MSG_T>(conf, idManager,vertexDataManager, dstOids, dstLids, newEdgeData, nbrPositions, numOfEdges, startLid, endLid);
    }

}
