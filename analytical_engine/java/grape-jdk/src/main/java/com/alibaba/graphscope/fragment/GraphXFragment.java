package com.alibaba.graphscope.fragment;

import static com.alibaba.graphscope.utils.CppClassName.GRAPHX_FRAGMENT;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_GRAPHX_GRAPHX_FRAGMENT_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.PropertyNbrUnit;
import com.alibaba.graphscope.ds.Vertex;

/**
 * We extend Immutable edgecut fragment just to reuse the interface, no annotations should be
 * inherited.
 */
@FFIGen
@CXXHead(CORE_JAVA_GRAPHX_GRAPHX_FRAGMENT_H)
@CXXHead(system = "stdint.h")
@FFITypeAlias(GRAPHX_FRAGMENT)
public interface GraphXFragment<OID_T, VID_T, VD_T, ED_T> extends
    EdgecutFragment<OID_T, VID_T, VD_T, ED_T> {

    @FFINameAlias("GetBegin")
    @CXXReference PropertyNbrUnit<VID_T> getBegin(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetEnd")
    @CXXReference PropertyNbrUnit<VID_T> getEnd(@CXXReference Vertex<VID_T> vertex);
//    @FFINameAlias("GetIncomingAdjList")
//    @CXXReference
//    PropertyRawAdjList<VID_T> getRawAdjList(@CXXReference Vertex<VID_T> vertex);
//
//    @FFINameAlias("GetOutgoingAdjList")
//    @CXXValue
//    PropertyRawAdjList<VID_T> getOutgoingAdjList(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("SetData")
    void setData(@CXXReference Vertex<VID_T> vertex, @CXXReference VD_T val);
}
