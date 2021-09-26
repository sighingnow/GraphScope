package com.alibaba.grape.sample.traverse;

import com.alibaba.grape.app.DefaultAppBase;
import com.alibaba.grape.app.DefaultContextBase;
import com.alibaba.grape.ds.AdjList;
import com.alibaba.grape.ds.Nbr;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.ds.VertexRange;
import com.alibaba.grape.fragment.ImmutableEdgecutFragment;
import com.alibaba.grape.parallel.DefaultMessageManager;
import com.alibaba.grape.sample.sssp.mirror.SSSPEdata;
import com.alibaba.grape.sample.sssp.mirror.SSSPOid;
import com.alibaba.grape.sample.sssp.mirror.SSSPVdata;

/**
 * @author xiaolei.zl
 * @date 2021/06/14
 */
public class TraverseMirror implements DefaultAppBase<SSSPOid, Long, SSSPVdata, SSSPEdata, TraverseMirrorDefaultContext> {
    @Override
    public void PEval(ImmutableEdgecutFragment<SSSPOid, Long, SSSPVdata, SSSPEdata> fragment,
                      DefaultContextBase defaultContextBase, DefaultMessageManager messageManager) {
        TraverseMirrorDefaultContext ctx = (TraverseMirrorDefaultContext) defaultContextBase;
        int innerVerteicesEnd = fragment.getInnerVerticesNum().intValue();
//        for (Vertex<Long> vertex = innerVertices.begin();
//             vertex.GetValue().intValue() != innerVerteicesEnd; vertex.inc()){
        VertexRange<Long> innerVertices = fragment.innerVertices();
        for (Vertex<Long> vertex : innerVertices.locals()) {
            AdjList<Long, SSSPEdata> adjList = fragment.getOutgoingAdjList(vertex);
            for (Nbr<Long, SSSPEdata> cur : adjList) {
                ctx.fake_edata = cur.data().value();
                ctx.fake_vid = cur.neighbor().GetValue();
            }
        }

        ctx.step += 1;
        messageManager.ForceContinue();
    }

    @Override
    public void IncEval(ImmutableEdgecutFragment<SSSPOid, Long, SSSPVdata, SSSPEdata> fragment,
                        DefaultContextBase defaultContextBase, DefaultMessageManager messageManager) {
        TraverseMirrorDefaultContext ctx = (TraverseMirrorDefaultContext) defaultContextBase;
        if (ctx.step >= ctx.maxStep) {
            return;
        }
        VertexRange<Long> innerVertices = fragment.innerVertices();
        for (Vertex<Long> vertex : innerVertices.locals()) {
            AdjList<Long, SSSPEdata> adjList = fragment.getOutgoingAdjList(vertex);
            for (Nbr<Long, SSSPEdata> cur : adjList) {
                ctx.fake_edata = cur.data().value();
                ctx.fake_vid = cur.neighbor().GetValue();
            }
        }

        ctx.step += 1;
        messageManager.ForceContinue();
    }
}
