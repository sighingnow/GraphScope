package com.alibaba.grape.fragment;


import com.alibaba.ffi.*;
import com.alibaba.grape.ds.ProjectedAdjList;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.ds.VertexRange;

import static com.alibaba.grape.utils.CPP_CLASSES_STRINGS.ARROW_PROJECTED_FRAGMENT;
import static com.alibaba.grape.utils.CPP_CLASSES_STRINGS.GRAPE_EMPTY_TYPE;
import static com.alibaba.grape.utils.CPP_HEADER_STRINGS.ARROW_PROJECTED_FRAGMENT_H;
import static com.alibaba.grape.utils.CPP_HEADER_STRINGS.CORE_JAVA_TYPE_ALIAS_H;
import static com.alibaba.grape.utils.CPP_LIBRARY_STRINGS.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(ARROW_PROJECTED_FRAGMENT_H)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@CXXHead(system = "stdint.h")
@FFITypeAlias(ARROW_PROJECTED_FRAGMENT)
@CXXTemplate(cxx = {"int64_t", "uint64_t", GRAPE_EMPTY_TYPE, "int64_t"},
        java = {"Long", "Long", "com.alibaba.grape.ds.EmptyType", "Long"})
@CXXTemplate(cxx = {"int64_t", "uint64_t", "double", "int64_t"},
        java = {"Long", "Long", "Double", "Long"})
@CXXTemplate(cxx = {"int64_t", "uint64_t", "int64_t", "int64_t"},
        java = {"Long", "Long", "Long", "Long"})
//TODO: vdata = string
public interface ArrowProjectedFragment<OID_T, VID_T, VDATA_T, EDATA_T> extends FFIPointer {
    int fid();

    int fnum();

    @FFINameAlias("Vertices") @CXXValue VertexRange<VID_T> vertices();

    @FFINameAlias("InnerVertices") @CXXValue VertexRange<VID_T> innerVertices();

    @FFINameAlias("OuterVertices") @CXXValue VertexRange<VID_T> outerVertices();

    @FFINameAlias("OuterVertices") @CXXValue VertexRange<VID_T> outerVertices(int fid);

    @FFINameAlias("GetVertex") boolean getVertex(@CXXReference OID_T oid, @CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetId") @CXXValue OID_T getId(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetFragId") int getFragId(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetData") @CXXValue VDATA_T getData(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("Gid2Vertex") boolean gid2Vertex(VID_T gid, @CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("Vertex2Gid") VID_T vertex2Gid(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetInnerVerticesNum") VID_T getInnerVerticesNum();

    @FFINameAlias("GetOuterVerticesNum") VID_T getOuterVerticesNum();

    @FFINameAlias("GetVerticesNum") VID_T getVerticesNum();

    @FFINameAlias("GetEdgeNum") long getEdgeNum();

    @FFINameAlias("GetTotalVerticesNum") long getTotalVerticesNum();

    @FFINameAlias("IsInnerVertex") boolean isInnerVertex(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("IsOuterVertex") boolean isOuterVertex(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetInnerVertex") boolean getInnerVertex(@CXXReference OID_T oid, @CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetOuterVertex") boolean getOuterVertex(@CXXReference OID_T oid, @CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetInnerVertexId") @CXXValue OID_T getInnerVertexId(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetOuterVertexId") @CXXValue OID_T getOuterVertexId(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("Gid2Oid") @CXXValue OID_T gid2Oid(VID_T gid);

    @FFINameAlias("Oid2Gid") VID_T oid2Gid(@CXXReference OID_T oid);

    @FFINameAlias("InnerVertexGid2Vertex") boolean innerVertexGid2Vertex(VID_T gid, @CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("OuterVertexGid2Vertex") boolean outerVertexGid2Vertex(VID_T gid, @CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetOuterVertexGid") VID_T getOuterVertexGid(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetInnerVertexGid") VID_T getInnerVertexGid(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetIncomingAdjList") @CXXValue ProjectedAdjList<VID_T, EDATA_T> getIncomingAdjList(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetOutgoingAdjList") @CXXValue ProjectedAdjList<VID_T, EDATA_T> getOutgoingAdjList(@CXXReference Vertex<VID_T> vertex);
}
