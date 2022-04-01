package com.alibaba.graphscope.graph;

/**
 * base interface for graphx and giraph vertexIdManager
 * @param <VID_T>
 * @param <OID_T>
 */
public interface VertexIdManager<VID_T, OID_T > {

    OID_T lid2Oid(VID_T lid);

    VID_T oid2Lid(OID_T oid);
}
