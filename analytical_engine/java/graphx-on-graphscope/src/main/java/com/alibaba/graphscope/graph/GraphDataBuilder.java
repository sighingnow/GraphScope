package com.alibaba.graphscope.graph;

import java.io.IOException;

public interface GraphDataBuilder<VD,ED> {
    void reserveVertex(int numVertices);

    void reserveEdge(int numEdges);

    void addEdge(long srcOid, long dstOid, ED edata, int threadId) throws IOException;

    void addVertex(long oid, VD vdata, int threadId) throws IOException;

    void finishAdding();

    long verticesAdded();

    long edgeAdded();
}
