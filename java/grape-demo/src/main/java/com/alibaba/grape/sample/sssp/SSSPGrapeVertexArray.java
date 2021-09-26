package com.alibaba.grape.sample.sssp;

import com.alibaba.ffi.CXXValueScope;
import com.alibaba.grape.app.DefaultAppBase;
import com.alibaba.grape.app.DefaultContextBase;
import com.alibaba.grape.app.lineparser.RecordLineParser;
import com.alibaba.grape.ds.*;
import com.alibaba.grape.fragment.ImmutableEdgecutFragment;
import com.alibaba.grape.graph.context.MutationContext;
import com.alibaba.grape.parallel.DefaultMessageManager;
import com.aliyun.odps.io.WritableRecord;

import java.io.IOException;

public class SSSPGrapeVertexArray implements DefaultAppBase<Long, Long, Long, Double, SSSPGrapeVertexDefaultContext> {
    public static class SSSPLoader implements RecordLineParser<Long, Long, Double> {
        @Override
        public void load(Long recordNum, WritableRecord record,
                         MutationContext<Long, Long, Double> context) throws IOException {
            Long from_oid = Long.valueOf(record.get("a").toString());
            Long to_oid = Long.valueOf(record.get("b").toString());
            context.addVertexSimple(from_oid, 0L);
            context.addVertexSimple(to_oid, 0L);

            double doubleValue = Double.valueOf(record.get("e").toString());
            context.addEdgeRequest(from_oid, to_oid, doubleValue);
        }
    }

    @Override
    public void PEval(ImmutableEdgecutFragment<Long, Long, Long, Double> frag, DefaultContextBase defaultContextBase,
                      DefaultMessageManager mm) {
        SSSPGrapeVertexArrayDefaultContext ctx = (SSSPGrapeVertexArrayDefaultContext) defaultContextBase;

        ctx.execTime -= System.nanoTime();

        VertexArray<Double, Long> partialResults = ctx.getPartialResults();
        VertexSet curModified = ctx.getCurModified();
        VertexSet nextModified = ctx.getNextModified();

        nextModified.clear();
        Vertex<Long> source = frag.innerVertices().begin();
        boolean sourceInThisFrag = frag.getInnerVertex(ctx.getSourceOid(), source);
        System.out.println("source in this frag?" + frag.fid() + ", " + sourceInThisFrag
                + ", lid: " + source.GetValue());
        if (sourceInThisFrag) {
            partialResults.set(source, 0.0);
            AdjList<Long, Double> adjList = frag.getOutgoingAdjList(source);
            for (Nbr<Long, Double> nbr : adjList) {
                Vertex<Long> cur = nbr.neighbor();
                partialResults.set(cur, Math.min(partialResults.get(cur), nbr.data()));
                if (frag.isOuterVertex(cur)) {
                    mm.syncStateOnOuterVertex(frag, cur, partialResults.get(cur));
                } else {
                    nextModified.set(cur);
                }
            }
        }
        ctx.execTime += System.nanoTime();

        ctx.postProcessTime -= System.nanoTime();
        mm.ForceContinue();
        curModified.assign(nextModified);
        ctx.postProcessTime += System.nanoTime();
    }

    @Override
    public void IncEval(ImmutableEdgecutFragment<Long, Long, Long, Double> frag, DefaultContextBase context,
                        DefaultMessageManager messageManager) {
        SSSPGrapeVertexArrayDefaultContext ctx = (SSSPGrapeVertexArrayDefaultContext) context;

        ctx.receiveMessageTIme -= System.nanoTime();
        receiveMessage(ctx, frag, messageManager);
        ctx.receiveMessageTIme += System.nanoTime();

        ctx.execTime -= System.nanoTime();
        execute(ctx, frag);
        ctx.execTime += System.nanoTime();

        ctx.sendMessageTime -= System.nanoTime();
        sendMessage(ctx, frag, messageManager);
        ctx.sendMessageTime += System.nanoTime();

        ctx.postProcessTime -= System.nanoTime();
        if (!ctx.nextModified.partialEmpty(0, frag.getInnerVerticesNum().intValue())) {
            messageManager.ForceContinue();
        }
        // nextModified.swap(curModified);
        ctx.curModified.assign(ctx.nextModified);
        ctx.postProcessTime += System.nanoTime();
    }

    private void receiveMessage(SSSPGrapeVertexArrayDefaultContext ctx, ImmutableEdgecutFragment<Long, Long, Long, Double> frag, DefaultMessageManager messageManager) {
        ctx.nextModified.clear();
        Vertex<Long> curVertex = frag.innerVertices().begin();
//        DoubleMsg msg = DoubleMsg.factory.create();
        double msg = 0.0;
        try (CXXValueScope scope = new CXXValueScope()) {
            while (messageManager.getMessage(frag, curVertex, msg)) {
                long curLid = curVertex.GetValue();
                if (ctx.partialResults.get(curVertex) > msg) {
                    ctx.partialResults.set(curVertex, msg);
                    ctx.curModified.set(curLid);
                }
            }
        }
    }

    private void execute(SSSPGrapeVertexArrayDefaultContext ctx, ImmutableEdgecutFragment<Long, Long, Long, Double> frag) {
//        BitSet curModifyBS = ctx.curModified.getBitSet();
//        Bitset curModifyBS = ctx.curModified.GetBitset();
        VertexRange<Long> innerVertices = frag.innerVertices();
        for (Vertex<Long> vertex : innerVertices.locals()) {
//    int innerVerteicesEnd = innerVertices.end().GetValue().intValue();
//    for (Vertex<Long> vertex = innerVertices.begin();
//         vertex.GetValue().intValue() != innerVerteicesEnd; vertex.inc()) {
            int vertexLid = vertex.GetValue().intValue();
            if (ctx.curModified.get(vertexLid)) {
                double curDist = ctx.partialResults.get(vertex);
                AdjList<Long, Double> adjList = frag.getOutgoingAdjList(vertex);
//        AdjList<Long,Double> adjList = frag.GetOutgoingAdjList(vertex);
                for (Nbr<Long, Double> nbr : adjList) {
//        long endPointerAddr = adjList.end().getAddress();
//        long nbrSize = adjList.begin().elementSize();
//        for (Nbr<Long, Double> nbr = adjList.begin(); nbr.getAddress() != endPointerAddr;
//             nbr.addV(nbrSize)) {
                    long curLid = nbr.neighbor().GetValue();
                    Vertex<Long> nbrVertex = nbr.neighbor();
                    double nextDist = curDist + nbr.data();
                    if (nextDist < ctx.partialResults.get(nbrVertex)) {
                        ctx.partialResults.set(nbrVertex, nextDist);
                        ctx.nextModified.set(curLid);
                    }
                }
            }
        }
    }

    private void sendMessage(SSSPGrapeVertexArrayDefaultContext ctx, ImmutableEdgecutFragment<Long, Long, Long, Double> frag, DefaultMessageManager messageManager) {
//        BitSet nextModifyBS = ctx.nextModified.getBitSet();
//        Bitset nextModifyBS = ctx.nextModified.GetBitset();
        VertexRange<Long> outerVertices = frag.outerVertices();
        for (Vertex<Long> vertex : outerVertices.locals()) {
//    int outerVerticesEnd = outerVertices.end().GetValue().intValue();
//      for (Vertex<Long> vertex = outerVertices.begin();
//           vertex.GetValue().intValue() != outerVerticesEnd; vertex.inc()) {
            if (ctx.nextModified.get(vertex.GetValue().intValue())) {
                messageManager.syncStateOnOuterVertex(frag, vertex, ctx.partialResults.get(vertex));
            }
        }
    }
}
