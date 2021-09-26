package com.alibaba.grape.sample.traverse;

import com.alibaba.ffi.CXXValueScope;
import com.alibaba.grape.app.DefaultAppBase;
import com.alibaba.grape.app.DefaultContextBase;
import com.alibaba.grape.ds.AdjList;
import com.alibaba.grape.ds.Nbr;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.ds.VertexRange;
import com.alibaba.grape.fragment.ImmutableEdgecutFragment;
import com.alibaba.grape.parallel.DefaultMessageManager;

/**
 * @author xiaolei.zl
 * @date 2021/06/14
 */
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
