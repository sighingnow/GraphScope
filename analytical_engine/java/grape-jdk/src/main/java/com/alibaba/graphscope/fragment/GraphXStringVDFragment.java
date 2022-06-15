package com.alibaba.graphscope.fragment;

import static com.alibaba.graphscope.utils.CppClassName.GRAPHX_FRAGMENT;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_GRAPHX_GRAPHX_FRAGMENT_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.ImmutableTypedArray;
import com.alibaba.graphscope.ds.PropertyNbrUnit;
import com.alibaba.graphscope.ds.StringTypedArray;
import com.alibaba.graphscope.ds.Vertex;

@FFIGen
@CXXHead(CORE_JAVA_GRAPHX_GRAPHX_FRAGMENT_H)
@CXXHead(system = "stdint.h")
@FFITypeAlias(GRAPHX_FRAGMENT)
public interface GraphXStringVDFragment<OID_T, VID_T, VD_T, ED_T>
    extends EdgecutFragment<OID_T, VID_T, VD_T, ED_T> {

    @FFINameAlias("GetIEBegin")
    PropertyNbrUnit<VID_T> getIEBegin(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetIEEnd")
    PropertyNbrUnit<VID_T> getIEEnd(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetOEBegin")
    PropertyNbrUnit<VID_T> geOEBegin(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetOEEnd")
    PropertyNbrUnit<VID_T> getOEEnd(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetEdataArray") @CXXReference ImmutableTypedArray<ED_T> getEdataArray();

    @FFINameAlias("GetVdataArray") @CXXReference StringTypedArray getVdataArray();

    @FFINameAlias("GetInEdgeNum")
    long getInEdgeNum();

    @FFINameAlias("GetOutEdgeNum")
    long getOutEdgeNum();
}
