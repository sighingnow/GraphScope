package com.alibaba.grape.sample.traverse;

import com.alibaba.fastffi.CXXValueScope;
import com.alibaba.grape.app.DefaultAppBase;
import com.alibaba.grape.app.DefaultContextBase;
import com.alibaba.grape.ds.AdjList;
import com.alibaba.grape.ds.Nbr;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.fragment.ImmutableEdgecutFragment;
import com.alibaba.grape.parallel.DefaultMessageManager;
import com.alibaba.grape.utils.FFITypeFactoryhelper;

/**
 * @author xiaolei.zl
 * @date 2021/06/14
 */
public class TraverseWhile implements DefaultAppBase<Long, Long, Long, Double, TraverseWhileContext> {
    @Override
    public void PEval(ImmutableEdgecutFragment<Long, Long, Long, Double> fragment,
                      DefaultContextBase defaultContextBase, DefaultMessageManager messageManager) {
        TraverseWhileContext ctx = (TraverseWhileContext) defaultContextBase;
        int innerVerteicesEnd = fragment.getInnerVerticesNum().intValue();
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
//        for (Vertex<Long> vertex = innerVertices.begin();
//             vertex.GetValue().intValue() != innerVerteicesEnd; vertex.inc()){
        try (CXXValueScope scope = new CXXValueScope()) {
            for (int i = 0; i < innerVerteicesEnd; ++i) {
                vertex.SetValue((long) i);
                AdjList<Long, Double> adjList = fragment.getOutgoingAdjList(vertex);
                Nbr<Long, Double> cur = adjList.begin();
//                Nbr<Long, Double> cur = fragment.GetOutgoingAdjListBegin(i);
                long elementSize = cur.elementSize();
                long endPointerAddr = adjList.end().getAddress();
//                long endPointerAddr = fragment.GetOutgoingAdjListEnd(i).getAddress();
                while (cur.getAddress() != endPointerAddr) {
                    ctx.fake_edata = cur.data();
                    ctx.fake_vid = cur.neighbor().GetValue();
                    cur.addV(elementSize);
                }
            }
        }
        ctx.step += 1;
        messageManager.ForceContinue();
    }

    @Override
    public void IncEval(ImmutableEdgecutFragment<Long, Long, Long, Double> fragment,
                        DefaultContextBase defaultContextBase, DefaultMessageManager messageManager) {
        TraverseWhileContext ctx = (TraverseWhileContext) defaultContextBase;
        if (ctx.step >= ctx.maxStep) {
            return;
        }
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        int innerVerteicesEnd = fragment.getInnerVerticesNum().intValue();
        try (CXXValueScope scope = new CXXValueScope()) {
            for (int i = 0; i < innerVerteicesEnd; ++i) {
                vertex.SetValue((long) i);
                AdjList<Long, Double> adjList = fragment.getOutgoingAdjList(vertex);
                Nbr<Long, Double> cur = adjList.begin();
//                Nbr<Long, Double> cur = fragment.GetOutgoingAdjListBegin(i);
                long elementSize = cur.elementSize();
                long endPointerAddr = adjList.end().getAddress();
//                long endPointerAddr = fragment.GetOutgoingAdjListEnd(i).getAddress();
                while (cur.getAddress() != endPointerAddr) {
                    ctx.fake_edata = cur.data();
                    ctx.fake_vid = cur.neighbor().GetValue();
                    cur.addV(elementSize);
                }
            }
        }
        ctx.step += 1;
        messageManager.ForceContinue();
    }
}
