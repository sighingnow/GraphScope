package com.alibaba.graphscope.graph;

import org.apache.spark.graphx.EdgeTriplet;
import scala.Function1;
import scala.Function2;
import scala.Tuple2;
import scala.collection.Iterator;

public interface EdgeManager<ED> {

    /**
     * Iterator over edges start from srcLid, update dstId info in context, and apply functions to context;
     * @param srcLid src lid
     * @param context edge context to use
     * @param msgSender mapping from edge triplet to a iterator for (dstId, msg).
     * @param msgMerger combiner
     * @param <VD> vertex data
     * @param <MSG_T> message type
     */
    <VD,MSG_T> void iterateOnEdges(long srcLid, EdgeContextImpl<?,ED,?> context, Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG_T>>> msgSender,
        Function2<MSG_T,MSG_T, MSG_T> msgMerger);
}
