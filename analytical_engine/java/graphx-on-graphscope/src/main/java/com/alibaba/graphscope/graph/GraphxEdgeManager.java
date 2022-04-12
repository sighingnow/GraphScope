package com.alibaba.graphscope.graph;

import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graphx.GSEdgeTriplet;
import com.alibaba.graphscope.mm.MessageStore;
import org.apache.spark.graphx.EdgeTriplet;
import scala.Function1;
import scala.Function2;
import scala.Tuple2;
import scala.collection.Iterator;

public interface GraphxEdgeManager<VD,ED,MSG_T> {

    void init(IFragment<Long,Long,VD,ED> fragment);

    /**
     * Iterator over edges start from srcLid, update dstId info in context, and apply functions to
     * context;
     *
     * @param srcLid    src lid
     * @param context   edge context to use
     * @param msgSender mapping from edge triplet to a iterator for (dstId, msg).
     */
     void iterateOnEdges(long srcLid, GSEdgeTriplet<VD, ED> context,
        Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG_T>>> msgSender,
         MessageStore<MSG_T,VD> outMessageStore);
}
