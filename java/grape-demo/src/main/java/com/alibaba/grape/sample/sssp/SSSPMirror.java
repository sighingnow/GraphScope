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

package com.alibaba.grape.sample.sssp;

import com.alibaba.fastffi.CXXValueScope;
import com.alibaba.grape.app.DefaultAppBase;
import com.alibaba.grape.app.DefaultContextBase;
import com.alibaba.grape.app.lineparser.EVLineParserBase;
import com.alibaba.grape.ds.*;
import com.alibaba.grape.fragment.ImmutableEdgecutFragment;
import com.alibaba.grape.graph.context.MutationContext;
import com.alibaba.grape.parallel.DefaultMessageManager;
import com.alibaba.grape.parallel.message.DoubleMsg;
import com.alibaba.grape.sample.sssp.mirror.SSSPEdata;
import com.alibaba.grape.sample.sssp.mirror.SSSPOid;
import com.alibaba.grape.sample.sssp.mirror.SSSPVdata;
import com.alibaba.grape.utils.DoubleArrayWrapper;
import com.alibaba.grape.utils.FFITypeFactoryhelper;

import java.io.IOException;

public class SSSPMirror implements DefaultAppBase<SSSPOid, Long, SSSPVdata, SSSPEdata, SSSPMirrorDefaultContext> {
    public static class MirrorLineParser implements EVLineParserBase<SSSPOid, SSSPVdata, SSSPEdata> {
        public SSSPOid oid = SSSPOid.create();
        public SSSPOid from = SSSPOid.create();
        public SSSPOid to = SSSPOid.create();
        public SSSPVdata vdata = SSSPVdata.create();
        public SSSPEdata edata = SSSPEdata.create();

        @Override
        public void loadVertexLine(String s, MutationContext<SSSPOid, SSSPVdata, SSSPEdata> mutationContext) throws IOException {
            String[] splited = s.split("\\s+");
            oid.value(Long.valueOf(splited[0]));
            vdata.value(0L);
            mutationContext.addVertexSimple(oid, vdata);
        }

        @Override
        public void loadEdgeLine(String s, MutationContext<SSSPOid, SSSPVdata, SSSPEdata> mutationContext) throws IOException {
            String[] splited = s.split("\\s+");
            from.value(Long.valueOf(splited[0]));
            to.value(Long.valueOf(splited[1]));

            edata.value(Double.valueOf(splited[2]));
            mutationContext.addEdgeRequest(from, to, edata);
        }
    }


    @Override
    public void PEval(ImmutableEdgecutFragment<SSSPOid, Long, SSSPVdata, SSSPEdata> frag, DefaultContextBase defaultContextBase,
                      DefaultMessageManager mm) {
        SSSPMirrorDefaultContext ctx = (SSSPMirrorDefaultContext) defaultContextBase;

        ctx.execTime -= System.nanoTime();

        DoubleArrayWrapper partialResults = ctx.getPartialResults();
        VertexSet curModified = ctx.getCurModified();
        VertexSet nextModified = ctx.getNextModified();

        nextModified.clear();
        Vertex<Long> source = frag.innerVertices().begin();
        boolean sourceInThisFrag = frag.getInnerVertex(ctx.getSourceOid(), source);
        System.out.println("source in this frag?" + frag.fid() + ", " + sourceInThisFrag
                + ", lid: " + source.GetValue());
        DoubleMsg msg = FFITypeFactoryhelper.newDoubleMsg();
        if (sourceInThisFrag) {
            partialResults.set(source.GetValue(), 0.0);
            AdjList<Long, SSSPEdata> adjList = frag.getOutgoingAdjList(source);
            for (Nbr<Long, SSSPEdata> nbr : adjList) {
                Vertex<Long> neigbor = nbr.neighbor();
                partialResults.set(neigbor, Math.min(partialResults.get(neigbor), nbr.data().value()));
                if (frag.isOuterVertex(neigbor)) {
                    //TODO: test template function
                    msg.setData(partialResults.get(neigbor));
                    mm.syncStateOnOuterVertex(frag, neigbor, msg);
                } else {
                    nextModified.set(neigbor);
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
    public void IncEval(ImmutableEdgecutFragment<SSSPOid, Long, SSSPVdata, SSSPEdata> frag, DefaultContextBase context,
                        DefaultMessageManager messageManager) {
        SSSPMirrorDefaultContext ctx = (SSSPMirrorDefaultContext) context;

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

    private void receiveMessage(SSSPMirrorDefaultContext ctx, ImmutableEdgecutFragment<SSSPOid, Long, SSSPVdata, SSSPEdata> frag, DefaultMessageManager messageManager) {
        ctx.nextModified.clear();
        Vertex<Long> curVertex = frag.innerVertices().begin();
        DoubleMsg msg = DoubleMsg.factory.create();
        try (CXXValueScope scope = new CXXValueScope()) {
            while (messageManager.getMessage(frag, curVertex, msg)) {
                long curLid = curVertex.GetValue();
                if (ctx.partialResults.get(curLid) > msg.getData()) {
                    ctx.partialResults.set(curLid, msg.getData());
                    ctx.curModified.set(curLid);
                }
            }
        }
    }

    private void execute(SSSPMirrorDefaultContext ctx, ImmutableEdgecutFragment<SSSPOid, Long, SSSPVdata, SSSPEdata> frag) {
//        BitSet curModifyBS = ctx.curModified.getBitSet();
        VertexRange<Long> innerVertices = frag.innerVertices();
        for (Vertex<Long> vertex : innerVertices.locals()) {
            int vertexLid = vertex.GetValue().intValue();
            if (ctx.curModified.get(vertexLid)) {
                double curDist = ctx.partialResults.get(vertexLid);
                AdjList<Long, SSSPEdata> adjList = frag.getOutgoingAdjList(vertex);
                for (Nbr<Long, SSSPEdata> nbr : adjList) {
                    long curLid = nbr.neighbor().GetValue();
                    double nextDist = curDist + nbr.data().value();
                    if (nextDist < ctx.partialResults.get(curLid)) {
                        ctx.partialResults.set(curLid, nextDist);
                        ctx.nextModified.set(curLid);
                    }
                }
            }
        }
    }

    private void sendMessage(SSSPMirrorDefaultContext ctx, ImmutableEdgecutFragment<SSSPOid, Long, SSSPVdata, SSSPEdata> frag, DefaultMessageManager messageManager) {
//        BitSet nextModifyBS = ctx.nextModified.getBitSet();
        VertexRange<Long> outerVertices = frag.outerVertices();
        DoubleMsg msg = FFITypeFactoryhelper.newDoubleMsg();
        for (Vertex<Long> vertex : outerVertices.locals()) {
            if (ctx.nextModified.get(vertex.GetValue().intValue())) {
                msg.setData(ctx.partialResults.get(vertex));
                messageManager.syncStateOnOuterVertex(frag, vertex, msg);
            }
        }
    }
}
