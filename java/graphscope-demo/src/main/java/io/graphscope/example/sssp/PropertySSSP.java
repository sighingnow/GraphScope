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

package io.graphscope.example.sssp;

import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.ds.VertexRange;
import com.alibaba.grape.parallel.message.DoubleMsg;
import com.alibaba.grape.utils.FFITypeFactoryhelper;
import io.graphscope.app.PropertyDefaultAppBase;
import io.graphscope.column.DoubleColumn;
import io.graphscope.context.PropertyDefaultContextBase;
import io.graphscope.ds.PropertyAdjList;
import io.graphscope.ds.PropertyNbr;
import io.graphscope.fragment.ArrowFragment;
import io.graphscope.parallel.PropertyMessageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertySSSP implements PropertyDefaultAppBase<Long, PropertySSSPContext> {
    public static Logger logger = LoggerFactory.getLogger(PropertySSSP.class.getName());

    @Override
    public void PEval(ArrowFragment<Long> fragment, PropertyDefaultContextBase<Long> context, PropertyMessageManager messageManager) {
        PropertySSSPContext ctx = (PropertySSSPContext) context;
        int vertexLabelNum = fragment.vertexLabelNum();
        int edgeLabelNum = fragment.edgeLabelNum();
        Vertex<Long> source = FFITypeFactoryhelper.newVertexLong();
        boolean sourceInThisFrag = false;

        for (int i = 0; i < vertexLabelNum; ++i) {
            sourceInThisFrag = fragment.getInnerVertex(i, ctx.sourceOid, source);
            if (sourceInThisFrag) {
                break;
            }
        }
        if (sourceInThisFrag) {
            int label = fragment.vertexLabel(source);
            ctx.partialResults.get(label).set(source, 0.0);
        } else {
            return;
        }

        for (int j = 0; j < edgeLabelNum; ++j) {
            PropertyAdjList<Long> adjList = fragment.getOutgoingAdjList(source, j);
            logger.info("outgoing adjlist " + adjList.begin().neighbor().GetValue() + ", " + adjList.end().neighbor().GetValue() + ", size " + adjList.size());
            for (PropertyNbr<Long> nbr : adjList.iterator()) {
                Vertex<Long> vertex = nbr.neighbor();
                double curDist = nbr.getInt(0);
                int vertexLabel = fragment.vertexLabel(vertex);
                if (ctx.partialResults.get(vertexLabel).get(vertex) > curDist) {
                    ctx.partialResults.get(vertexLabel).set(vertex, curDist);
                    ctx.nextModified.get(vertexLabel).set(vertex);
                }
            }
        }

        DoubleMsg msg = DoubleMsg.factory.create();
        for (int i = 0; i < vertexLabelNum; ++i) {
            VertexRange<Long> outerVertices = fragment.outerVertices(i);
            for (Vertex<Long> vertex : outerVertices.locals()) {
                if (ctx.nextModified.get(i).get(vertex)) {
                    msg.setData(ctx.partialResults.get(i).get(vertex));
                    messageManager.syncStateOnOuterVertex(fragment, vertex, msg);
                    ctx.nextModified.get(i).set(vertex, false);
                }
            }
        }

        for (int i = 0; i < vertexLabelNum; ++i) {
            VertexRange<Long> innerVertices = fragment.innerVertices(i);
            boolean ok = false;
            for (Vertex<Long> vertex : innerVertices.locals()) {
                if (ctx.nextModified.get(i).get(vertex)) {
                    messageManager.ForceContinue();
                    ok = true;
                    break;
                }
            }
            if (ok) {
                break;
            }
        }
        for (int i = 0; i < vertexLabelNum; ++i) {
            ctx.curModified.get(i).assign(ctx.nextModified.get(i));
            ctx.nextModified.get(i).clear();
        }
        //update changes to dist column
        for (int i = 0; i < vertexLabelNum; ++i) {
            VertexRange<Long> innerVertices = fragment.innerVertices(i);
            DoubleColumn column = ctx.getDoubleColumn(i, ctx.distColumnIndices.get(i));
            for (Vertex<Long> vertex : innerVertices) {
                column.set(vertex, ctx.partialResults.get(i).get(vertex));
            }
        }
    }

    @Override
    public void IncEval(ArrowFragment<Long> fragment, PropertyDefaultContextBase<Long> context, PropertyMessageManager messageManager) {
        PropertySSSPContext ctx = (PropertySSSPContext) context;
        {
            Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
            DoubleMsg msg = DoubleMsg.factory.create();
            while (messageManager.getMessage(fragment, vertex, msg)) {
                int vertexLabel = fragment.vertexLabel(vertex);
                if (ctx.partialResults.get(vertexLabel).get(vertex) > msg.getData()) {
                    ctx.partialResults.get(vertexLabel).set(vertex, msg.getData());
                    ctx.curModified.get(vertexLabel).set(vertex);
                }
            }
        }


        int vertexLabelNum = fragment.vertexLabelNum();
        int edgeLabelNum = fragment.edgeLabelNum();
        long data_sum = 0;
        for (int i = 0; i < vertexLabelNum; ++i) {
            VertexRange<Long> innerVertices = fragment.innerVertices(i);
            for (Vertex<Long> vertex : innerVertices.locals()) {
                if (!ctx.curModified.get(i).get(vertex)) {
                    continue;
                }
                ctx.curModified.get(i).set(vertex, false);
                double dist = ctx.partialResults.get(i).get(vertex);
                for (int j = 0; j < edgeLabelNum; ++j) {
                    PropertyAdjList<Long> adjList = fragment.getOutgoingAdjList(vertex, j);
                    for (PropertyNbr<Long> nbr : adjList.iterator()) {
                        Vertex<Long> nbrVertex = nbr.neighbor();
                        int nbrVertexLabel = fragment.vertexLabel(nbrVertex);
                        double nextDist = dist + nbr.getInt(0);
                        data_sum += nextDist;
                        if (nextDist < ctx.partialResults.get(nbrVertexLabel).get(nbrVertex)) {
                            ctx.partialResults.get(nbrVertexLabel).set(nbrVertex, nextDist);
                            ctx.nextModified.get(nbrVertexLabel).set(nbrVertex);
                        }
                    }
                }
            }
        }
        logger.info("IncEval " + data_sum);
        //sync out vertices

        DoubleMsg msg = DoubleMsg.factory.create();
        for (int i = 0; i < vertexLabelNum; ++i) {
            VertexRange<Long> outerVertices = fragment.outerVertices(i);
            for (Vertex<Long> vertex : outerVertices.locals()) {
                if (ctx.nextModified.get(i).get(vertex)) {
                    msg.setData(ctx.partialResults.get(i).get(vertex));
                    messageManager.syncStateOnOuterVertex(fragment, vertex, msg);
                    ctx.nextModified.get(i).set(vertex, false);
                }
            }
        }

        //check condition to move forward
        for (int i = 0; i < vertexLabelNum; ++i) {
            VertexRange<Long> innerVertices = fragment.innerVertices(i);
            boolean ok = false;
            for (Vertex<Long> vertex : innerVertices.locals()) {
                if (ctx.nextModified.get(i).get(vertex)) {
                    messageManager.ForceContinue();
                    ok = true;
                    break;
                }
            }
            if (ok) {
                break;
            }
        }

        for (int i = 0; i < vertexLabelNum; ++i) {
            ctx.curModified.get(i).assign(ctx.nextModified.get(i));
            ctx.nextModified.get(i).clear();
        }
        //update changes to dist column
        for (int i = 0; i < vertexLabelNum; ++i) {
            VertexRange<Long> innerVertices = fragment.innerVertices(i);
            DoubleColumn column = ctx.getDoubleColumn(i, ctx.distColumnIndices.get(i));
            for (Vertex<Long> vertex : innerVertices) {
                column.set(vertex, ctx.partialResults.get(i).get(vertex));
            }
        }
    }
}
