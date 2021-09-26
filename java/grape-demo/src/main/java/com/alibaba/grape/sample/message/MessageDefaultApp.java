package com.alibaba.grape.sample.message;

import com.alibaba.grape.app.DefaultAppBase;
import com.alibaba.grape.app.DefaultContextBase;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.ds.VertexRange;
import com.alibaba.grape.fragment.ImmutableEdgecutFragment;
import com.alibaba.grape.parallel.DefaultMessageManager;


public class MessageDefaultApp implements DefaultAppBase<Long, Long, Long, Double, MessageDefaultContext> {
    @Override
    public void PEval(ImmutableEdgecutFragment<Long, Long, Long, Double> fragment, DefaultContextBase defaultContextBase, DefaultMessageManager messageManager) {
        MessageDefaultContext ctx = (MessageDefaultContext) defaultContextBase;
        VertexRange<Long> outerVertices = fragment.outerVertices();
        for (Vertex<Long> vertex : outerVertices.locals()) {
            messageManager.syncStateOnOuterVertex(fragment, vertex, fragment.getData(vertex) + 1);
        }
        ctx.step += 1;
        messageManager.ForceContinue();
    }

    @Override
    public void IncEval(ImmutableEdgecutFragment<Long, Long, Long, Double> fragment, DefaultContextBase defaultContextBase, DefaultMessageManager messageManager) {
        MessageDefaultContext ctx = (MessageDefaultContext) defaultContextBase;
        if (ctx.step >= ctx.maxStep) {
            return;
        }
        {
            Long msg = new Long(1L);
            Vertex<Long> curVertex = fragment.innerVertices().begin();
            while (messageManager.getMessage(fragment, curVertex, msg)) {
                //process with the msg
                ctx.numMsgReceived += 1;
            }
            System.out.println("last received msg" + msg);
            ctx.receiveMsgTime += System.nanoTime();
        }
        for (Vertex<Long> vertex : fragment.outerVertices().locals()) {
            messageManager.syncStateOnOuterVertex(fragment, vertex, fragment.getData(vertex) + 1);
        }
        ctx.step += 1;
        messageManager.ForceContinue();
    }
}
