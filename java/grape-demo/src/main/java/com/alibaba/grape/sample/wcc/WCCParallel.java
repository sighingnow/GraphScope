package com.alibaba.grape.sample.wcc;

import com.alibaba.grape.app.ParallelAppBase;
import com.alibaba.grape.app.ParallelContextBase;
import com.alibaba.grape.ds.AdjList;
import com.alibaba.grape.ds.Nbr;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.ds.VertexRange;
import com.alibaba.grape.fragment.ImmutableEdgecutFragment;
import com.alibaba.grape.parallel.ParallelMessageManager;
import com.alibaba.grape.parallel.message.DoubleMsg;
import com.alibaba.grape.parallel.message.LongMsg;
import com.alibaba.grape.utils.FFITypeFactoryhelper;

import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class WCCParallel implements ParallelAppBase<Long, Long, Long, Double, WCCParallelContext> {
    private void PropagateLabelPush(ImmutableEdgecutFragment<Long, Long, Long, Double> frag, WCCParallelContext ctx, ParallelMessageManager mm) {
        VertexRange<Long> innerVertices = frag.innerVertices();
        VertexRange<Long> outerVertices = frag.outerVertices();
        BiConsumer<Vertex<Long>, Integer> consumer = (vertex, finalTid) -> {
            long cid = ctx.comp_id.get(vertex);
//            AdjListv2<Long, Double> adjListv2 = frag.GetOutgoingAdjListV2(vertex);
            AdjList<Long, Double> adjListv2 = frag.getOutgoingAdjList(vertex);
            for (Nbr<Long, Double> nbr : adjListv2) {
                Vertex<Long> cur = nbr.neighbor();
                if (nbr.neighbor().GetValue().intValue() > frag.getVerticesNum().intValue()) {
                    System.out.println("accessing vertex " + vertex.GetValue() + " nbr " + nbr.neighbor().GetValue().intValue()
                            + " num vertices " + frag.getVerticesNum().intValue());
                    System.out.println("try to get oid" + frag.getId(nbr.neighbor()));
                }
                if (Long.compareUnsigned(ctx.comp_id.get(cur), cid) > 0) {
                    ctx.comp_id.compareAndSetMinUnsigned(cur, cid);
                    ctx.nextModified.set(cur);
                }
            }
        };
        forEachVertex(innerVertices, ctx.threadNum, ctx.executor, ctx.currModified, consumer);


        BiConsumer<Vertex<Long>, LongMsg> filler = (vertex, msg) -> {
            msg.setData(ctx.comp_id.get(vertex));
        };
//        forEachVertex(outerVertices, ctx.threadNum, ctx.executor, ctx.nextModified, consumer2);
        BiConsumer<Vertex<Long>, Integer> msgSender = (vertex, finalTid) -> {
            DoubleMsg msg = FFITypeFactoryhelper.newDoubleMsg(ctx.comp_id.get(vertex));
            mm.syncStateOnOuterVertex(frag, vertex, msg, finalTid);
        };
        forEachVertex(outerVertices, ctx.threadNum, ctx.executor, ctx.nextModified, msgSender);
        //mm.ParallelSyncStateOnOuterVertex(outerVertices, ctx.nextModified, ctx.threadNum, ctx.executor, filler);
    }

    private void PropagateLabelPull(ImmutableEdgecutFragment<Long, Long, Long, Double> frag, WCCParallelContext ctx, ParallelMessageManager mm) {
        VertexRange<Long> innerVertices = frag.innerVertices();
        VertexRange<Long> outerVertices = frag.outerVertices();

        BiConsumer<Vertex<Long>, Integer> outgoing = (vertex, finalTid) -> {
            long oldCid = ctx.comp_id.get(vertex);
            long newCid = oldCid;
            AdjList<Long, Double> adjListv2 = frag.getOutgoingInnerVertexAdjList(vertex);
            for (Nbr<Long, Double> nbr : adjListv2) {
                long value = ctx.comp_id.get(nbr.neighbor());
                if (Long.compareUnsigned(value, newCid) < 0) {
                    newCid = value;
                }
            }
            if (Long.compareUnsigned(newCid, oldCid) < 0) {
                ctx.comp_id.set(vertex, newCid);
                ctx.nextModified.set(vertex);
            }
        };
        BiConsumer<Vertex<Long>, Integer> incoming = (vertex, finalTid) -> {
            long oldCid = ctx.comp_id.get(vertex);
            long newCid = oldCid;
//            AdjListv2<Long, Double> adjListv2 = frag.GetIncomingAdjListV2(vertex);
            AdjList<Long, Double> adjListv2 = frag.getIncomingAdjList(vertex);
            for (Nbr<Long, Double> nbr : adjListv2) {
                long value = ctx.comp_id.get(nbr.neighbor());
                if (Long.compareUnsigned(value, newCid) < 0) {
                    newCid = value;
                }
            }
            LongMsg msg = LongMsg.factory.create();
            if (Long.compareUnsigned(newCid, oldCid) < 0) {
                ctx.comp_id.set(vertex, newCid);
                ctx.nextModified.set(vertex);
                msg.setData(newCid);
                mm.syncStateOnOuterVertex(frag, vertex, msg, finalTid);
            }
        };
        forEachVertex(innerVertices, ctx.threadNum, ctx.executor, outgoing);
        forEachVertex(outerVertices, ctx.threadNum, ctx.executor, incoming);
    }

    @Override
    public void PEval(ImmutableEdgecutFragment<Long, Long, Long, Double> frag, ParallelContextBase context, ParallelMessageManager messageManager) {
        WCCParallelContext ctx = (WCCParallelContext) context;
        VertexRange<Long> innerVertices = frag.innerVertices();
        VertexRange<Long> outerVertices = frag.outerVertices();
        messageManager.initChannels(ctx.threadNum);

        BiConsumer<Vertex<Long>, Integer> consumerInner = (vertex, finalTid) -> {
            ctx.comp_id.set(vertex, frag.getInnerVertexGid(vertex));
        };
        BiConsumer<Vertex<Long>, Integer> consumerOuter = (vertex, finalTid) -> {
            ctx.comp_id.set(vertex, frag.getOuterVertexGid(vertex));
        };
        forEachVertex(innerVertices, ctx.threadNum, ctx.executor, consumerInner);
        forEachVertex(outerVertices, ctx.threadNum, ctx.executor, consumerOuter);

        PropagateLabelPull(frag, ctx, messageManager);

        if (!ctx.nextModified.partialEmpty(0, frag.getInnerVerticesNum().intValue())) {
            messageManager.ForceContinue();
        }
        ctx.currModified.assign(ctx.nextModified);
    }

    @Override
    public void IncEval(ImmutableEdgecutFragment<Long, Long, Long, Double> frag, ParallelContextBase context, ParallelMessageManager messageManager) {
        WCCParallelContext ctx = (WCCParallelContext) context;
        ctx.nextModified.clear();

        BiConsumer<Vertex<Long>, LongMsg> msgReceiveConsumer = (vertex, msg) -> {
            if (Long.compareUnsigned(ctx.comp_id.get(vertex), msg.getData()) > 0) {
                ctx.comp_id.compareAndSetMinUnsigned(vertex, msg.getData());
                ctx.currModified.set(vertex);
            }
        };
        Supplier<LongMsg> msgSupplier = () -> LongMsg.factory.create();
        messageManager.parallelProcess(frag, ctx.threadNum, ctx.executor, msgSupplier, msgReceiveConsumer);

        double rate = (double) ctx.currModified.getBitSet().cardinality() / ctx.innerVerticesNum;
        if (rate > 0.1) {
            PropagateLabelPull(frag, ctx, messageManager);
        } else {
            PropagateLabelPush(frag, ctx, messageManager);
        }

        if (!ctx.nextModified.partialEmpty(0, frag.getInnerVerticesNum().intValue())) {
            messageManager.ForceContinue();
        }
        ctx.currModified.assign(ctx.nextModified);
    }
}


