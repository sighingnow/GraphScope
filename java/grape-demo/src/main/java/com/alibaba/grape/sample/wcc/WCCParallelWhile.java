package com.alibaba.grape.sample.wcc;

import com.alibaba.grape.app.ParallelAppBase;
import com.alibaba.grape.app.ParallelContextBase;
import com.alibaba.grape.ds.AdjList;
import com.alibaba.grape.ds.Nbr;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.ds.VertexRange;
import com.alibaba.grape.fragment.ImmutableEdgecutFragment;
import com.alibaba.grape.parallel.MessageInBuffer;
import com.alibaba.grape.parallel.ParallelMessageManager;
import com.alibaba.grape.parallel.message.LongMsg;
import com.alibaba.grape.utils.FFITypeFactoryhelper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class WCCParallelWhile implements ParallelAppBase<Long, Long, Long, Double, WCCParallelContext> {
    private void PropagateLabelPush(ImmutableEdgecutFragment<Long, Long, Long, Double> frag, WCCParallelContext ctx, ParallelMessageManager mm) {
        VertexRange<Long> innerVertices = frag.innerVertices();
        VertexRange<Long> outerVertices = frag.outerVertices();
        {
            CountDownLatch countDownLatch = new CountDownLatch(ctx.threadNum);
            AtomicInteger atomicInteger = new AtomicInteger(innerVertices.begin().GetValue().intValue());
            int chunkSize = 1024;
            int originEnd = innerVertices.end().GetValue().intValue();
            for (int tid = 0; tid < ctx.threadNum; ++tid) {
                final int finalTid = tid;
                ctx.executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
                        while (true) {
                            int curBegin = Math.min(atomicInteger.getAndAdd(chunkSize), originEnd);
                            int curEnd = Math.min(curBegin + chunkSize, originEnd);
                            if (curBegin >= originEnd) {
                                break;
                            }
                            for (int i = curBegin; i < curEnd; ++i) {
                                if (ctx.currModified.get(i)) {
                                    vertex.SetValue((long) i);
                                    long cid = ctx.comp_id.get(vertex);
                                    AdjList<Long, Double> adjList = frag.getOutgoingAdjList(vertex);
                                    long endPointerAddr = adjList.end().getAddress();
                                    Nbr<Long, Double> nbr = adjList.begin();
                                    long elementSize = nbr.elementSize();
                                    for (; nbr.getAddress() != endPointerAddr; nbr.addV(elementSize)) {
                                        Vertex<Long> cur = nbr.neighbor();
                                        if (ctx.comp_id.get(cur) > cid) {
                                            ctx.comp_id.compareAndSetMin(cur, cid);
                                            ctx.nextModified.set(cur);
                                        }
                                    }
                                }
                            }
                        }
                        countDownLatch.countDown();
                    }
                });
            }
            try {
                countDownLatch.await();
            } catch (Exception e) {
                e.printStackTrace();
                ctx.executor.shutdown();
            }
        }
        {
            CountDownLatch countDownLatch = new CountDownLatch(ctx.threadNum);
            AtomicInteger atomicInteger = new AtomicInteger(outerVertices.begin().GetValue().intValue());
            int chunkSize = 1024;
            int originEnd = outerVertices.end().GetValue().intValue();
            for (int tid = 0; tid < ctx.threadNum; ++tid) {
                final int finalTid = tid;
                ctx.executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
                        LongMsg msg = LongMsg.factory.create();
                        while (true) {
                            int curBegin = Math.min(atomicInteger.getAndAdd(chunkSize), originEnd);
                            int curEnd = Math.min(curBegin + chunkSize, originEnd);
                            if (curBegin >= originEnd) {
                                break;
                            }
                            for (int i = curBegin; i < curEnd; ++i) {
                                if (ctx.nextModified.get(i)) {
                                    vertex.SetValue((long) i);
//                                    consumer.accept(vertex, finalTid);
                                    msg.setData(ctx.comp_id.get(vertex));
                                    mm.syncStateOnOuterVertex(frag, vertex, msg, finalTid);
                                }
                            }
                        }
                        countDownLatch.countDown();
                    }
                });
            }
            try {
                countDownLatch.await();
            } catch (Exception e) {
                e.printStackTrace();
                ctx.executor.shutdown();
            }
        }
//        forEachVertex(outerVertices, ctx.threadNum, ctx.executor, ctx.nextModified, consumer2);
//        mm.ParallelSyncStateOnOuterVertex(outerVertices, ctx.nextModified, ctx.threadNum, ctx.executor, filler);
    }

    private void PropagateLabelPull(ImmutableEdgecutFragment<Long, Long, Long, Double> frag, WCCParallelContext ctx, ParallelMessageManager mm) {
        VertexRange<Long> innerVertices = frag.innerVertices();
        VertexRange<Long> outerVertices = frag.outerVertices();
        {
            CountDownLatch countDownLatch = new CountDownLatch(ctx.threadNum);
            AtomicInteger atomicInteger = new AtomicInteger(innerVertices.begin().GetValue().intValue());
            int chunkSize = 1024;
            int originEnd = innerVertices.end().GetValue().intValue();
            for (int tid = 0; tid < ctx.threadNum; ++tid) {
                final int finalTid = tid;
                ctx.executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
                        while (true) {
                            int curBegin = Math.min(atomicInteger.getAndAdd(chunkSize), originEnd);
                            int curEnd = Math.min(curBegin + chunkSize, originEnd);
                            if (curBegin >= originEnd) {
                                break;
                            }
                            for (int i = curBegin; i < curEnd; ++i) {
                                vertex.SetValue((long) i);
                                long oldCid = ctx.comp_id.get(vertex);
                                long newCid = oldCid;
                                AdjList<Long, Double> adjList = frag.getOutgoingInnerVertexAdjList(vertex);
                                long endPointerAddr = adjList.end().getAddress();
                                Nbr<Long, Double> nbr = adjList.begin();
                                long elementSize = nbr.elementSize();
                                for (; nbr.getAddress() != endPointerAddr; nbr.addV(elementSize)) {
                                    long value = ctx.comp_id.get(nbr.neighbor());
                                    if (value < newCid) {
                                        newCid = value;
                                    }
                                }
                                if (newCid < oldCid) {
                                    ctx.comp_id.set(vertex, newCid);
                                    ctx.nextModified.set(vertex);
                                }
                            }
                        }
                        countDownLatch.countDown();
                    }
                });
            }
            try {
                countDownLatch.await();
            } catch (Exception e) {
                e.printStackTrace();
                ctx.executor.shutdown();
            }
        }
        {
            CountDownLatch countDownLatch = new CountDownLatch(ctx.threadNum);
            AtomicInteger atomicInteger = new AtomicInteger(outerVertices.begin().GetValue().intValue());
            int chunkSize = 1024;
            int originEnd = outerVertices.end().GetValue().intValue();
            for (int tid = 0; tid < ctx.threadNum; ++tid) {
                final int finalTid = tid;
                ctx.executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
                        while (true) {
                            int curBegin = Math.min(atomicInteger.getAndAdd(chunkSize), originEnd);
                            int curEnd = Math.min(curBegin + chunkSize, originEnd);
                            if (curBegin >= originEnd) {
                                break;
                            }
                            for (int i = curBegin; i < curEnd; ++i) {
                                vertex.SetValue((long) i);
                                long oldCid = ctx.comp_id.get(vertex);
                                long newCid = oldCid;
                                AdjList<Long, Double> adjList = frag.getIncomingAdjList(vertex);
                                long endPointerAddr = adjList.end().getAddress();
                                Nbr<Long, Double> nbr = adjList.begin();
                                long elementSize = nbr.elementSize();
                                for (; nbr.getAddress() != endPointerAddr; nbr.addV(elementSize)) {
                                    long value = ctx.comp_id.get(nbr.neighbor());
                                    if (value < newCid) {
                                        newCid = value;
                                    }
                                }
                                LongMsg msg = LongMsg.factory.create();
                                if (newCid < oldCid) {
                                    ctx.comp_id.set(vertex, newCid);
                                    ctx.nextModified.set(vertex);
                                    msg.setData(newCid);
                                    mm.syncStateOnOuterVertex(frag, vertex, msg, finalTid);
                                }
                            }
                        }
                        countDownLatch.countDown();
                    }
                });
            }
            try {
                countDownLatch.await();
            } catch (Exception e) {
                e.printStackTrace();
                ctx.executor.shutdown();
            }
        }
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
        {
            CountDownLatch countDownLatch = new CountDownLatch(ctx.threadNum);
            MessageInBuffer.Factory bufferFactory = FFITypeFactoryhelper.newMessageInBuffer();
            int chunkSize = 1024;
            for (int tid = 0; tid < ctx.threadNum; ++tid) {
                final int finalTid = tid;
                ctx.executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        MessageInBuffer messageInBuffer = bufferFactory.create();
                        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
                        LongMsg msg = LongMsg.factory.create();
                        boolean result;
                        while (true) {
                            synchronized (ParallelMessageManager.class) {
                                result = messageManager.getMessageInBuffer(messageInBuffer);
                            }
                            if (result) {
                                while (messageInBuffer.getMessage(frag, vertex, msg)) {
                                    if (ctx.comp_id.get(vertex) > msg.getData()) {
                                        ctx.comp_id.compareAndSetMin(vertex, msg.getData());
                                        ctx.currModified.set(vertex);
                                    }
                                }
                            } else {
                                break;
                            }
                        }
                        countDownLatch.countDown();
                    }
                });
            }
            try {
                countDownLatch.await();
            } catch (Exception e) {
                e.printStackTrace();
                ctx.executor.shutdown();
            }
        }

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


