package com.alibaba.graphscope.graph;

import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graphx.GSEdgeTriplet;
import com.alibaba.graphscope.mm.MessageStore;
import java.util.List;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeTriplet;
import scala.Function1;
import scala.Tuple2;
import scala.collection.Iterator;

public interface GraphxEdgeManager<VD,ED,MSG_T> {

    void init(IFragment<Long,Long,VD,ED> fragment, int numCores);

    scala.collection.Iterator<Edge<ED>> iterator(long startLid, long endLid);

    long getPartialEdgeNum(long startLid, long endLid);

    long getTotalEdgeNum();

    /**
     * Iterator over edges start from srcLid, update dstId info in context, and apply functions to
     * context;
     *
     * @param srcLid    src lid
     * @param context   edge context to use
     * @param msgSender mapping from edge triplet to a iterator for (dstId, msg).
     */
     void iterateOnEdges(long srcLid, GSEdgeTriplet<VD, ED> context,
        Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG_T>>> msgSender,MessageStore<MSG_T,VD> outMessageCache);

    void iterateOnEdgesParallel(int tid, long srcLid, GSEdgeTriplet<VD, ED> context,
        Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG_T>>> msgSender,MessageStore<MSG_T,VD> outMessageCache);

    /**
     * Create copy with new (can be different type) edge data.
     * @param <ED2> new edge data type
     * @return created edge manager
     */
    <ED2> GraphxEdgeManager<VD,ED2,MSG_T> withNewEdgeData(List<ED2> newEdgeData, long startLid, long endLid);
}
