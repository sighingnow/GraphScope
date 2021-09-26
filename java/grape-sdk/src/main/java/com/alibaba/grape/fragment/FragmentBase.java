package com.alibaba.grape.fragment;

import com.alibaba.ffi.CXXReference;
import com.alibaba.ffi.CXXValue;
import com.alibaba.ffi.FFINameAlias;
import com.alibaba.ffi.FFIPointer;
import com.alibaba.grape.ds.AdjList;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.ds.VertexRange;

public interface FragmentBase<OID_T, VID_T, VDATA_T, EDATA_T> extends FFIPointer {
    int fid();

    int fnum();

    @FFINameAlias("GetEdgeNum")
    long getEdgeNum();

    @FFINameAlias("GetVerticesNum")
    VID_T getVerticesNum();

    @FFINameAlias("GetTotalVerticesNum")
    long getTotalVerticesNum();

    @FFINameAlias("Vertices")
    @CXXValue VertexRange<VID_T> vertices();

    @FFINameAlias("GetVertex")
    boolean getVertex(@CXXReference OID_T oid, @CXXReference Vertex<VID_T> vertex);

    /**
     * Get the original Id
     *
     * @param vertex
     * @return OID
     */
    @FFINameAlias("GetId")
    @CXXValue OID_T getId(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetFragId")
    int getFragId(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetData")
    @CXXReference VDATA_T getData(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("SetData")
    void setData(@CXXReference Vertex<VID_T> vertex, @CXXReference VDATA_T val);

    @FFINameAlias("HasChild")
    boolean hasChild(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("HasParent")
    boolean hasParent(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetLocalInDegree")
    int getLocalInDegree(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetLocalOutDegree")
    int getLocalOutDegree(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("Gid2Vertex")
    boolean gid2Vertex(@CXXReference VID_T gid, @CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("Vertex2Gid")
    @CXXValue VID_T vertex2Gid(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetIncomingAdjList")
    @CXXValue AdjList<VID_T, EDATA_T> getIncomingAdjList(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetOutgoingAdjList")
    @CXXValue AdjList<VID_T, EDATA_T> getOutgoingAdjList(@CXXReference Vertex<VID_T> vertex);
}
