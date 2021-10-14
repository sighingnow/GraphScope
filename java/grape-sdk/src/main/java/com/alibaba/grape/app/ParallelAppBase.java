/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.grape.app;

import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.ds.VertexRange;
import com.alibaba.grape.ds.VertexSet;
import com.alibaba.grape.fragment.ImmutableEdgecutFragment;
import com.alibaba.grape.parallel.ParallelMessageManager;
import com.alibaba.grape.utils.FFITypeFactoryhelper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * All Paralle app should implement this class. This class contains the implementation of mutli threads implementation
 * for vertices traversal, while the message multi-threading should be handled by cpp.
 * <p>
 * The behavior should be consistent with cpp.
 *
 * @param <OID_T>
 * @param <VID_T>
 * @param <VDATA_T>
 * @param <EDATA_T>
 * @param <C>
 */
@SuppressWarnings("rawtypes")
public interface ParallelAppBase<OID_T, VID_T, VDATA_T, EDATA_T, C extends ParallelContextBase<OID_T, VID_T, VDATA_T, EDATA_T>>
        extends AppBase<OID_T, VID_T, VDATA_T, EDATA_T, C> {
    /**
     * Partial evaluation procedure to be implemented.
     * 
     * @param graph
     * @param context
     * @param messageManager
     */
    void PEval(ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T> graph, ParallelContextBase context,
            ParallelMessageManager messageManager);

    /**
     * Incremental Evaluation procedure to be implemented.
     * 
     * @param graph
     * @param context
     * @param messageManager
     */
    void IncEval(ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T> graph, ParallelContextBase context,
            ParallelMessageManager messageManager);

    /**
     * Iterate over vertexs in VertexRange, applying lambda functions on each vertex.
     * 
     * @param vertices
     * @param threadNum
     * @param executor
     * @param consumer
     */
    default void forEachVertex(VertexRange<Long> vertices, int threadNum, ExecutorService executor,
            BiConsumer<Vertex<Long>, Integer> consumer) {
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        AtomicInteger atomicInteger = new AtomicInteger(vertices.begin().GetValue().intValue());
        int chunkSize = 1024;
        int originEnd = vertices.end().GetValue().intValue();
        for (int tid = 0; tid < threadNum; ++tid) {
            final int finalTid = tid;
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    // Vertex<Long> vertex = vertices.begin();
                    Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
                    while (true) {
                        int curBegin = Math.min(atomicInteger.getAndAdd(chunkSize), originEnd);
                        int curEnd = Math.min(curBegin + chunkSize, originEnd);
                        if (curBegin >= originEnd) {
                            break;
                        }
                        try {
                            for (int i = curBegin; i < curEnd; ++i) {
                                vertex.SetValue((long) i);
                                consumer.accept(vertex, finalTid);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.err.println("origin end " + originEnd + " verteics " + curBegin + " " + curEnd
                                    + " vertex " + vertex.GetValue().intValue() + " thread " + finalTid);
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
            executor.shutdown();
        }
    }

    /**
     * Apply consumer for each vertex in vertices, in a parallel schema
     *
     * @param vertices
     * @param threadNum
     * @param executor
     * @param vertexSet
     *            A vertex set contains flags
     * @param consumer
     */
    default void forEachVertex(VertexRange<Long> vertices, int threadNum, ExecutorService executor, VertexSet vertexSet,
            BiConsumer<Vertex<Long>, Integer> consumer) {
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        AtomicInteger atomicInteger = new AtomicInteger(vertices.begin().GetValue().intValue());
        int chunkSize = 1024;
        int originEnd = vertices.end().GetValue().intValue();
        for (int tid = 0; tid < threadNum; ++tid) {
            final int finalTid = tid;
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    // Vertex<Long> vertex = vertices.begin();
                    Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
                    while (true) {
                        int curBegin = Math.min(atomicInteger.getAndAdd(chunkSize), originEnd);
                        int curEnd = Math.min(curBegin + chunkSize, originEnd);
                        if (curBegin >= originEnd) {
                            break;
                        }
                        for (int i = curBegin; i < curEnd; ++i) {
                            if (vertexSet.get(i)) {
                                vertex.SetValue((long) i);
                                consumer.accept(vertex, finalTid);
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
            executor.shutdown();
        }
    }
}