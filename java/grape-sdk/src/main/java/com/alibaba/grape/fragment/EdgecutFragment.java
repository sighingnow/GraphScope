package com.alibaba.grape.fragment;

import com.alibaba.ffi.CXXReference;
import com.alibaba.ffi.CXXValue;
import com.alibaba.ffi.FFINameAlias;
import com.alibaba.grape.ds.AdjList;
import com.alibaba.grape.ds.DestList;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.ds.VertexRange;

public interface EdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T>
        extends FragmentBase<OID_T, VID_T, VDATA_T, EDATA_T> {
    @FFINameAlias("GetInnerVerticesNum")
    @CXXValue VID_T getInnerVerticesNum();

    @FFINameAlias("GetOuterVerticesNum")
    @CXXValue VID_T getOuterVerticesNum();

    @FFINameAlias("InnerVertices")
    @CXXValue VertexRange<VID_T> innerVertices();

    @FFINameAlias("OuterVertices")
    @CXXValue VertexRange<VID_T> outerVertices();

    @FFINameAlias("IsInnerVertex")
    boolean isInnerVertex(@CXXReference Vertex<VID_T> v);

    @FFINameAlias("IsOuterVertex")
    boolean isOuterVertex(@CXXReference Vertex<VID_T> v);

    @FFINameAlias("GetInnerVertex")
    boolean getInnerVertex(@CXXReference OID_T oid, @CXXReference Vertex<VID_T> v);

    @FFINameAlias("GetOuterVertex")
    boolean getOuterVertex(@CXXReference OID_T oid, @CXXReference Vertex<VID_T> v);

    @FFINameAlias("GetInnerVertexId")
    @CXXValue OID_T getInnerVertexId(@CXXReference Vertex<VID_T> v);

    @FFINameAlias("GetOuterVertexId")
    @CXXValue OID_T getOuterVertexId(@CXXReference Vertex<VID_T> v);

    @FFINameAlias("InnerVertexGid2Vertex")
    boolean innerVertexGid2Vertex(@CXXReference VID_T gid, @CXXReference Vertex<VID_T> v);

    @FFINameAlias("OuterVertexGid2Vertex")
    boolean outerVertexGid2Vertex(@CXXReference VID_T gid, @CXXReference Vertex<VID_T> v);

    @FFINameAlias("GetOuterVertexGid")
    @CXXValue VID_T getOuterVertexGid(@CXXReference Vertex<VID_T> v);

    @FFINameAlias("GetInnerVertexGid")
    @CXXValue VID_T getInnerVertexGid(@CXXReference Vertex<VID_T> v);

    @FFINameAlias("IEDests")
    @CXXValue DestList inEdgeDests(@CXXReference Vertex<VID_T> v);

    @FFINameAlias("OEDests")
    @CXXValue DestList outEdgeDests(@CXXReference Vertex<VID_T> v);

    @FFINameAlias("IOEDests")
    @CXXValue DestList inOutEdgeDests(@CXXReference Vertex<VID_T> v);

    @FFINameAlias("GetIncomingInnerVertexAdjList")
    @CXXValue AdjList<VID_T, EDATA_T> getIncomingInnerVertexAdjList(@CXXReference Vertex<VID_T> v);

    @FFINameAlias("GetOutgoingInnerVertexAdjList")
    @CXXValue AdjList<VID_T, EDATA_T> getOutgoingInnerVertexAdjList(@CXXReference Vertex<VID_T> v);
}
