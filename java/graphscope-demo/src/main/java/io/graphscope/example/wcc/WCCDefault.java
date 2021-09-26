package io.graphscope.example.wcc;

import com.alibaba.grape.ds.GSVertexArray;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.ds.VertexRange;
import com.alibaba.grape.parallel.message.LongMsg;
import io.graphscope.app.PropertyDefaultAppBase;
import io.graphscope.context.PropertyDefaultContextBase;
import io.graphscope.ds.PropertyAdjList;
import io.graphscope.ds.PropertyNbr;
import io.graphscope.fragment.ArrowFragment;
import io.graphscope.parallel.PropertyMessageManager;

public class WCCDefault implements PropertyDefaultAppBase<Long, WCCDefaultContext> {
    @Override
    public void PEval(ArrowFragment<Long> fragment, PropertyDefaultContextBase<Long> context, PropertyMessageManager messageManager) {
        WCCDefaultContext ctx = (WCCDefaultContext) context;
        int vertexLabelNum = fragment.vertexLabelNum();
        int edgeLabelNum = fragment.edgeLabelNum();
        for (int i = 0; i < vertexLabelNum; ++i) {
            GSVertexArray<Long> curComID = ctx.compId.get(i);
            for (Vertex<Long> vertex : fragment.innerVertices(i).locals()) {
                curComID.set(vertex, fragment.getInnerVertexGid(vertex));
            }
            for (Vertex<Long> vertex : fragment.outerVertices(i).locals()) {
                curComID.set(vertex, fragment.getOuterVertexGid(vertex));
            }
        }

        //
        for (int i = 0; i < vertexLabelNum; ++i) {
            VertexRange<Long> innerVertices = fragment.innerVertices(i);
            for (Vertex<Long> vertex : innerVertices.locals()) {
                long vertexCompId = ctx.compId.get(i).get(vertex);
                for (int j = 0; j < edgeLabelNum; ++j) {
                    PropertyAdjList<Long> outgoingAdjList = fragment.getOutgoingAdjList(vertex, j);
                    for (PropertyNbr<Long> propertyNbr : outgoingAdjList.iterator()) {
                        Vertex<Long> nbrVertex = propertyNbr.neighbor();
                        int nbrLabel = fragment.vertexLabel(nbrVertex);
                        if (ctx.compId.get(nbrLabel).get(nbrVertex) > vertexCompId) {
                            ctx.compId.get(nbrLabel).set(nbrVertex, vertexCompId);
                            ctx.nextModified.get(nbrLabel).set(nbrVertex);
                        }
                    }
                    PropertyAdjList<Long> incomingAdjList = fragment.getIncomingAdjList(vertex, j);
                    for (PropertyNbr<Long> propertyNbr : incomingAdjList.iterator()) {
                        Vertex<Long> nbrVertex = propertyNbr.neighbor();
                        int nbrLabel = fragment.vertexLabel(nbrVertex);
                        if (ctx.compId.get(nbrLabel).get(nbrVertex) > vertexCompId) {
                            ctx.compId.get(nbrLabel).set(nbrVertex, vertexCompId);
                            ctx.nextModified.get(nbrLabel).set(nbrVertex);
                        }
                    }
                }
            }
        }
        LongMsg msg = LongMsg.factory.create();
        for (int i = 0; i < vertexLabelNum; ++i) {
            VertexRange<Long> outerVertices = fragment.outerVertices(i);
            for (Vertex<Long> vertex : outerVertices.locals()) {
                if (ctx.nextModified.get(i).get(vertex)) {
                    msg.setData(ctx.compId.get(i).get(vertex));
                    messageManager.syncStateOnOuterVertex(fragment, vertex, msg);
                    ctx.nextModified.get(i).set(vertex, false);
                }
            }
        }
        for (int i = 0; i < vertexLabelNum; ++i) {
            ctx.curModified.get(i).assign(ctx.nextModified.get(i));
            ctx.nextModified.get(i).clear();
        }
    }

    @Override
    public void IncEval(ArrowFragment<Long> graph, PropertyDefaultContextBase<Long> context, PropertyMessageManager messageManager) {
        WCCDefaultContext ctx = (WCCDefaultContext) context;
    }
}
