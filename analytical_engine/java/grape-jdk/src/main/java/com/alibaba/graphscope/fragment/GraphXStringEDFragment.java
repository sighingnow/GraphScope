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
public interface GraphXStringEDFragment<OID_T, VID_T, VD_T, ED_T>
    extends BaseGraphXFragment<OID_T, VID_T, VD_T, ED_T> {

    @FFINameAlias("GetEdataArray") @CXXReference StringTypedArray getEdataArray();

    @FFINameAlias("GetVdataArray") @CXXReference  ImmutableTypedArray<VD_T> getVdataArray();

    @FFINameAlias("GetData")
    @CXXReference
    VD_T getData(@CXXReference Vertex<VID_T> vertex);
}
