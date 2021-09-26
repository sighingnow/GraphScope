package com.alibaba.grape.sample.sssp;

import com.alibaba.grape.app.ParallelAppBase;
import com.alibaba.grape.app.ParallelContextBase;
import com.alibaba.grape.app.lineparser.RecordLineParser;
import com.alibaba.grape.ds.AdjList;
import com.alibaba.grape.ds.Nbr;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.ds.VertexSet;
import com.alibaba.grape.fragment.ImmutableEdgecutFragment;
import com.alibaba.grape.graph.context.MutationContext;
import com.alibaba.grape.parallel.ParallelMessageManager;
import com.alibaba.grape.parallel.message.DoubleMsg;
import com.alibaba.grape.utils.AtomicDoubleArrayWrapper;
import com.alibaba.grape.utils.FFITypeFactoryhelper;
import com.aliyun.odps.io.WritableRecord;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class SSSPParallel
        implements ParallelAppBase<Long, Long, Long, Double, SSSPParallelContext> {
    public static class SSSPLoader implements RecordLineParser<Long, Long, Double> {
        @Override
        public void load(Long recordNum, WritableRecord record,
                         MutationContext<Long, Long, Double> context) throws IOException {
            Long from_oid = Long.valueOf(record.get("a").toString());
            Long to_oid = Long.valueOf(record.get("b").toString());
            context.addVertexSimple(from_oid, 0L);
            context.addVertexSimple(to_oid, 0L);

            double doubleValue = Double.valueOf(record.get("e").toString());
            // Long edata = new Long((int) doubleValue);
            context.addEdgeRequest(from_oid, to_oid, doubleValue);
        }
    }

    @Override
    public void PEval(ImmutableEdgecutFragment<Long, Long, Long, Double> frag, ParallelContextBase contextBase,
                      ParallelMessageManager mm) {
        SSSPParallelContext context = (SSSPParallelContext) contextBase;
        mm.initChannels(context.thread_num());
        context.nextModified.clear();

        Vertex<Long> source = FFITypeFactoryhelper.newVertexLong();

        boolean sourceInThisFrag = frag.getInnerVertex(context.sourceOid, source);
        System.out.println("source in this frag?" + frag.fid() + ", " + sourceInThisFrag
                + ", lid: " + source.GetValue());

        AtomicDoubleArrayWrapper partialResults = context.partialResults;
        VertexSet curModified = context.curModified;
        VertexSet nextModified = context.nextModified;
        DoubleMsg msg = FFITypeFactoryhelper.newDoubleMsg();
        if (sourceInThisFrag) {
            partialResults.set(source, 0.0);
            AdjList<Long, Double> adjList = frag.getOutgoingAdjList(source);
            for (Nbr<Long, Double> nbr : adjList) {
                Vertex<Long> vertex = nbr.neighbor();
                partialResults.set(vertex, Math.min(nbr.data(), partialResults.get(vertex)));
                if (frag.isOuterVertex(vertex)) {
                    msg.setData(partialResults.get(vertex));
                    mm.syncStateOnOuterVertex(frag, vertex, msg, 0);
                } else {
                    nextModified.set(vertex);
                }
            }
        }
        mm.ForceContinue();
        curModified.assign(nextModified);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void IncEval(ImmutableEdgecutFragment<Long, Long, Long, Double> frag, ParallelContextBase contextBase,
                        ParallelMessageManager messageManager) {
        SSSPParallelContext context = (SSSPParallelContext) contextBase;
        context.nextModified.clear();


        // Parallel process the message with the support of JavaMessageInBuffer.
        context.receiveMessageTime -= System.nanoTime();
        receiveMessage(context, frag, messageManager);
        context.receiveMessageTime += System.nanoTime();

        // Do incremental calculation
        context.execTime -= System.nanoTime();
        execute(context, frag);
        context.execTime += System.nanoTime();

        context.sendMessageTime -= System.nanoTime();
        sendMessage(context, frag, messageManager);
        context.sendMessageTime += System.nanoTime();

        if (!context.nextModified.partialEmpty(0, frag.getInnerVerticesNum().intValue())) {
            messageManager.ForceContinue();
        }
        context.curModified.assign(context.nextModified);
    }

    private void receiveMessage(SSSPParallelContext context,
                                ImmutableEdgecutFragment<Long, Long, Long, Double> frag,
                                ParallelMessageManager messageManager) {

        Supplier<DoubleMsg> msgSupplier = () -> DoubleMsg.factory.create();
        BiConsumer<Vertex<Long>, DoubleMsg> messageConsumer = (vertex, msg) -> {
            double preValue = context.partialResults.get(vertex);
            if (preValue > msg.getData()) {
                context.partialResults.compareAndSetMin(vertex, msg.getData());
                context.curModified.set(vertex);
            }
        };
        messageManager.parallelProcess(frag, context.threadNum, context.executor, msgSupplier, messageConsumer);
    }

    private void execute(SSSPParallelContext context,
                         ImmutableEdgecutFragment<Long, Long, Long, Double> frag) {
        int innerVerticesNum = frag.getInnerVerticesNum().intValue();

        BiConsumer<Vertex<Long>, Integer> consumer = (vertex, finalTid) -> {
            double curDist = context.partialResults.get(vertex);
            AdjList<Long, Double> nbrs = frag.getOutgoingAdjList(vertex);
            for (Nbr<Long, Double> nbr : nbrs) {
                long curLid = nbr.neighbor().GetValue();
                double nextDist = curDist + nbr.data();
                if (nextDist < context.partialResults.get(curLid)) {
                    context.partialResults.compareAndSetMin(curLid, nextDist);
                    context.nextModified.set(curLid);
                }
            }
        };
        forEachVertex(frag.innerVertices(), context.threadNum, context.executor, context.curModified, consumer);
    }

    private void sendMessage(SSSPParallelContext context,
                             ImmutableEdgecutFragment<Long, Long, Long, Double> frag,
                             ParallelMessageManager messageManager) {
        // for outer vertices sync data
        BiConsumer<Vertex<Long>, Integer> msgSender = (vertex, finalTid) -> {
            DoubleMsg msg = FFITypeFactoryhelper.newDoubleMsg(context.partialResults.get(vertex));
            messageManager.syncStateOnOuterVertex(frag, vertex, msg, finalTid);
        };
        forEachVertex(frag.outerVertices(), context.threadNum, context.executor, context.nextModified, msgSender);
    }
}
