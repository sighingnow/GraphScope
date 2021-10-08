package io.graphscope.example.projected;

import com.alibaba.grape.ds.ProjectedAdjList;
import com.alibaba.grape.ds.ProjectedNbr;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.ds.VertexRange;
import com.alibaba.grape.fragment.ArrowProjectedFragment;
import com.alibaba.grape.parallel.DefaultMessageManager;
import com.alibaba.grape.parallel.message.LongMsg;
import com.alibaba.grape.utils.FFITypeFactoryhelper;
import io.graphscope.app.ProjectedDefaultAppBase;
import io.graphscope.context.ProjectedDefaultContextBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSSPProjected implements ProjectedDefaultAppBase<Long, Long, Double, Long, SSSPProjectedContext> {
    public static Logger logger = LoggerFactory.getLogger(SSSPProjected.class.getName());

    @Override
    public void PEval(ArrowProjectedFragment<Long, Long, Double, Long> fragment, ProjectedDefaultContextBase<ArrowProjectedFragment<Long, Long, Double, Long>> context, DefaultMessageManager messageManager) {
        logger.info("innervertices: " + fragment.innerVertices().begin().GetValue() + "," + fragment.innerVertices().end().GetValue());
        logger.info("edgenum : " + fragment.getEdgeNum() + " vertices num: " + fragment.getVerticesNum());
        logger.info("gid for oid 4: " + fragment.oid2Gid(4L));
        logger.info("inner v num: " + fragment.getInnerVerticesNum());
        SSSPProjectedContext ctx = (SSSPProjectedContext) context;
        Vertex<Long> source = FFITypeFactoryhelper.newVertexLong();
        boolean sourceInThisFrag = false;

        sourceInThisFrag = fragment.getInnerVertex(ctx.sourceOid, source);
        logger.info("result " + sourceInThisFrag + ", " + ctx.sourceOid + ", " + source.GetValue());
        if (sourceInThisFrag) {
            ctx.partialResults.set(source, 0L);
        } else {
            return;
        }

        ProjectedAdjList<Long, Long> adjList = fragment.getOutgoingAdjList(source);
        logger.info("outgoing adjlist " + adjList.begin().neighbor().GetValue() + ", " + adjList.end().neighbor().GetValue() + ", size " + adjList.size());
        long iterTime = 0;
        for (ProjectedNbr<Long, Long> nbr : adjList.iterator()) {
            Vertex<Long> vertex = nbr.neighbor();
            long curDist = nbr.data();
            long prev = ctx.partialResults.get(vertex);
            if (ctx.partialResults.get(vertex) > curDist) {
                ctx.partialResults.set(vertex, curDist);
                ctx.nextModified.set(vertex);
            }
            iterTime += 1;
            //logger.info("vertex<" + vertex.GetValue() + ">nbrdata :" + curDist + ", prev: " + prev + " after: " + ctx.partialResults.get(vertex));
        }
        logger.info("peval iteration " + iterTime);

        LongMsg msg = LongMsg.factory.create();
        VertexRange<Long> outerVertices = fragment.outerVertices();
        for (Vertex<Long> vertex : outerVertices.locals()) {
            if (ctx.nextModified.get(vertex)) {
                msg.setData(ctx.partialResults.get(vertex));
                messageManager.syncStateOnOuterVertex(fragment, vertex, msg);
                ctx.nextModified.set(vertex, false);
            }
        }

        VertexRange<Long> innerVertices = fragment.innerVertices();
        for (Vertex<Long> vertex : innerVertices.locals()) {
            if (ctx.nextModified.get(vertex)) {
                messageManager.ForceContinue();
                break;
            }
        }
        ctx.curModified.assign(ctx.nextModified);
        ctx.nextModified.clear();
    }

    @Override
    public void IncEval(ArrowProjectedFragment<Long, Long, Double, Long> fragment, ProjectedDefaultContextBase<ArrowProjectedFragment<Long, Long, Double, Long>> context, DefaultMessageManager messageManager) {
        SSSPProjectedContext ctx = (SSSPProjectedContext) context;
        {
            Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
            LongMsg msg = LongMsg.factory.create();
            while (messageManager.getMessage(fragment, vertex, msg)) {
                if (ctx.partialResults.get(vertex) > msg.getData()) {
                    ctx.partialResults.set(vertex, msg.getData());
                    ctx.curModified.set(vertex);
                }
            }
        }
        long data_sum = 0;
        VertexRange<Long> innerVertices = fragment.innerVertices();
        for (Vertex<Long> vertex : innerVertices.locals()) {
            if (!ctx.curModified.get(vertex)) {
                continue;
            }
            ctx.curModified.set(vertex, false);
            long dist = ctx.partialResults.get(vertex);
            ProjectedAdjList<Long, Long> adjList = fragment.getOutgoingAdjList(vertex);
            long expectedIter = adjList.size();
            long actualIter = 0;
            for (ProjectedNbr<Long, Long> nbr : adjList.iterator()) {
                Vertex<Long> nbrVertex = nbr.neighbor();
                actualIter += 1;
                long nextDist = dist + nbr.data();
                data_sum += nextDist;
                if (nextDist < ctx.partialResults.get(nbrVertex)) {
                    ctx.partialResults.set(nbrVertex, nextDist);
                    ctx.nextModified.set(nbrVertex);
                }
            }
            if (expectedIter != actualIter) {
                logger.info("adjlist iteration false: " + vertex.GetValue());
            }
        }
        logger.info("IncEval " + data_sum);

        //sync out vertices
        LongMsg msg = LongMsg.factory.create();
        VertexRange<Long> outerVertices = fragment.outerVertices();
        for (Vertex<Long> vertex : outerVertices.locals()) {
            if (ctx.nextModified.get(vertex)) {
                msg.setData(ctx.partialResults.get(vertex));
                messageManager.syncStateOnOuterVertex(fragment, vertex, msg);
                ctx.nextModified.set(vertex, false);
            }
        }

        //check condition to move forward
        for (Vertex<Long> vertex : innerVertices.locals()) {
            if (ctx.nextModified.get(vertex)) {
                messageManager.ForceContinue();
                break;
            }
        }
        ctx.curModified.assign(ctx.nextModified);
        ctx.nextModified.clear();
    }
}
