package com.alibaba.graphscope.graph.impl;

import com.alibaba.graphscope.conf.GraphXConf;
import com.alibaba.graphscope.fragment.IFragment;
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

public class GraphxEdgeManagerUpdateEData<VD, ED, MSG> extends GraphxEdgeManagerImpl<VD, ED, MSG> {

    private static Logger logger = LoggerFactory.getLogger(
        GraphxEdgeManagerUpdateEData.class.getName());

    private long startLid, endLid;
    private int firstPos;
    private List<ED> newEdatas;

    public GraphxEdgeManagerUpdateEData(GraphXConf conf,
        VertexIdManager<Long, Long> idManager,
        VertexDataManager<VD> vertexDataManager,
        Long[] dstOids, Long[] dstLids, List<ED> edatas, int[] nbrPositions,
        long[] numOfEdges, long startLid, long endLid) {
        super(conf, idManager, vertexDataManager);
        this.dstLids = dstLids;
        this.dstOids = dstOids;
        this.newEdatas = edatas;
        this.nbrPositions = nbrPositions;
        this.numOfEdges = numOfEdges;
        this.startLid = startLid;
        this.endLid = endLid;
        this.firstPos = nbrPositions[(int) startLid];
        if (getPartialEdgeNum(startLid, endLid) != newEdatas.size()) {
            throw new IllegalStateException(
                "when create GraphxEdgeManagerUpdateEdata, the input edata array's length should be same with override edges"
                    + getPartialEdgeNum(startLid, endLid) + ", array len: " + newEdatas.size());
        }
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
                edge.setAttr(newEdatas.get(curPos - firstPos));
                //	logger.info("src{}, dst{}}", dstOids[curPos], edatas[curPos]);
                curPos += 1;
                return edge;
            }
        };
    }

    @Override
    public void iterateOnEdgesParallel(int threadId, long srcLid, GSEdgeTriplet<VD, ED> triplet,
        Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG>>> msgSender,
        MessageStore<MSG, VD> outMessageCache) {
        long numEdge = numOfEdges[(int) srcLid];
        int nbrPos = nbrPositions[(int) srcLid];
        int endPos = (int) (nbrPos + numEdge);
        for (int i = nbrPos; i < endPos; ++i) {
            triplet.setDstOid(dstOids[i], vertexDataManager.getVertexData(dstLids[i]), edatas[i - firstPos]);
            Iterator<Tuple2<Long, MSG>> iterator = msgSender.apply(triplet);
            logger.info("for edge: {}->{}", triplet.srcId(), triplet.dstId());
            while (iterator.hasNext()) {
                Tuple2<Long, MSG> tuple2 = iterator.next();
                outMessageCache.addOidMessage(tuple2._1(), tuple2._2());
            }
        }
    }

    @Override
    public void init(IFragment<Long, Long, VD, ED> fragment, int numCores) {
        throw new IllegalStateException("For update edata, we should never call init");
    }

    @Override
    public <ED2> GraphxEdgeManager<VD, ED2, MSG> withNewEdgeData(List<ED2> newEdgeData, long startLid, long endLid) {
        if (newEdgeData.size() != getPartialEdgeNum(startLid, endLid)){
            throw new IllegalStateException("new Edata length: " + newEdgeData.size() + ", expected: " + getPartialEdgeNum(startLid, endLid));
        }
        if (startLid != this.startLid || endLid != this.endLid){
            throw new IllegalStateException("unmatched range: " + startLid + "," + endLid + " vs " + this.startLid + ", " +this.endLid);
        }
        return new GraphxEdgeManagerUpdateEData<VD,ED2,MSG>(conf, idManager, vertexDataManager, dstOids, dstLids, newEdgeData, nbrPositions, numOfEdges, startLid, endLid);
    }
}
