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

package com.alibaba.grape.sample.traverse;

import com.alibaba.fastffi.CXXValueScope;
import com.alibaba.grape.app.DefaultAppBase;
import com.alibaba.grape.app.DefaultContextBase;
import com.alibaba.grape.ds.AdjList;
import com.alibaba.grape.ds.Nbr;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.ds.VertexRange;
import com.alibaba.grape.fragment.ImmutableEdgecutFragment;
import com.alibaba.grape.parallel.DefaultMessageManager;

public class Traverse implements DefaultAppBase<Long, Long, Long, Double, TraverseDefaultContext> {
    @Override
    public void PEval(ImmutableEdgecutFragment<Long, Long, Long, Double> fragment,
            DefaultContextBase defaultContextBase, DefaultMessageManager messageManager) {
        TraverseDefaultContext ctx = (TraverseDefaultContext) defaultContextBase;
        VertexRange<Long> innerVertices = fragment.innerVertices();
        for (Vertex<Long> vertex : innerVertices.locals()) {
            AdjList<Long, Double> adjList = fragment.getOutgoingAdjList(vertex);
            for (Nbr<Long, Double> cur : adjList) {
                ctx.fake_edata = cur.data();
                ctx.fake_vid = cur.neighbor().GetValue();
            }
        }

        ctx.step += 1;
        messageManager.ForceContinue();
    }

    @Override
    public void IncEval(ImmutableEdgecutFragment<Long, Long, Long, Double> fragment,
            DefaultContextBase defaultContextBase, DefaultMessageManager messageManager) {
        TraverseDefaultContext ctx = (TraverseDefaultContext) defaultContextBase;
        if (ctx.step >= ctx.maxStep) {
            return;
        }
        VertexRange<Long> innerVertices = fragment.innerVertices();
        try (CXXValueScope scope = new CXXValueScope()) {
            for (Vertex<Long> vertex : innerVertices.locals()) {
                AdjList<Long, Double> adjList = fragment.getOutgoingAdjList(vertex);
                for (Nbr<Long, Double> cur : adjList) {
                    ctx.fake_edata = cur.data();
                    ctx.fake_vid = cur.neighbor().GetValue();
                }
            }
        }

        ctx.step += 1;
        messageManager.ForceContinue();
    }
}
