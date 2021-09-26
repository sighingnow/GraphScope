package com.alibaba.grape.sample.traverse;

import com.alibaba.grape.app.ParallelAppBase;
import com.alibaba.grape.app.ParallelContextBase;
import com.alibaba.grape.ds.AdjList;
import com.alibaba.grape.ds.Nbr;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.fragment.ImmutableEdgecutFragment;
import com.alibaba.grape.parallel.ParallelMessageManager;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author xiaolei.zl
 * @date 2021/06/14
 */
public class TraverseParallel implements ParallelAppBase<Long, Long, Long, Double, TraverseParallelContext> {
    @Override
    public void PEval(ImmutableEdgecutFragment<Long, Long, Long, Double> fragment,
                      ParallelContextBase contextBase, ParallelMessageManager messageManager) {
        TraverseParallelContext ctx = (TraverseParallelContext) contextBase;
        CountDownLatch latch = new CountDownLatch(ctx.threadNum);
        int innerVerteicesEnd = fragment.getInnerVerticesNum().intValue();
        AtomicLong cur = new AtomicLong(0L);
        for (int i = 0; i < ctx.threadNum; ++i) {
            ctx.executor.execute(new Runnable() {
                @Override
                public void run() {
                    long start = Math.min(cur.getAndAdd(ctx.chunkSize), innerVerteicesEnd);
                    long end = Math.min(start + ctx.chunkSize, innerVerteicesEnd);
                    Vertex<Long> vertex = fragment.innerVertices().begin();
                    for (long lid = start; lid < end; ++lid) {
                        vertex.SetValue(lid);
                        AdjList<Long, Double> adjList = fragment.getOutgoingAdjList(vertex);
                        for (Nbr<Long, Double> cur : adjList) {
                            ctx.fake_edata = cur.data();
                            ctx.fake_vid = cur.neighbor().GetValue();
                        }
                    }
                    latch.countDown();
                }
            });
        }
        try {
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
            ctx.executor.shutdown();
        }
        ctx.step += 1;
        messageManager.ForceContinue();
    }

    @Override
    public void IncEval(ImmutableEdgecutFragment<Long, Long, Long, Double> fragment,
                        ParallelContextBase contextBase, ParallelMessageManager messageManager) {
        TraverseParallelContext ctx = (TraverseParallelContext) contextBase;
        if (ctx.step >= ctx.maxStep) {
            ctx.executor.shutdown();
            return;
        }
        CountDownLatch latch = new CountDownLatch(ctx.threadNum);
        int innerVerteicesEnd = fragment.getInnerVerticesNum().intValue();
        AtomicLong cur = new AtomicLong(0L);
        for (int i = 0; i < ctx.threadNum; ++i) {
            ctx.executor.execute(new Runnable() {
                @Override
                public void run() {
                    long fake_vid = 0L;
                    double fake_edata = 0.0;
                    while (true) {
                        long start = Math.min(cur.getAndAdd(ctx.chunkSize), innerVerteicesEnd);
                        long end = Math.min(start + ctx.chunkSize, innerVerteicesEnd);
                        Vertex<Long> vertex = fragment.innerVertices().begin();
                        if (start >= innerVerteicesEnd) {
                            break;
                        }
                        for (long lid = start; lid < end; ++lid) {
                            vertex.SetValue(lid);
                            AdjList<Long, Double> adjList = fragment.getOutgoingAdjList(vertex);
                            for (Nbr<Long, Double> cur : adjList) {
                                fake_vid = cur.neighbor().GetValue();
                                fake_edata = cur.data();
                            }
                        }
                    }
                    ctx.fake_edata = fake_edata;
                    ctx.fake_vid = fake_vid;
                    latch.countDown();
                }
            });
        }
        try {
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
            ctx.executor.shutdown();
        }
        ctx.step += 1;
        messageManager.ForceContinue();
    }
}
