package com.alibaba.graphscope.fragment;

import static com.alibaba.graphscope.utils.CppClassName.GRAPHX_FRAGMENT;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_GRAPHX_GRAPHX_FRAGMENT_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.StringTypedArray;

@FFIGen
@CXXHead(CORE_JAVA_GRAPHX_GRAPHX_FRAGMENT_H)
@CXXHead(system = "stdint.h")
@FFITypeAlias(GRAPHX_FRAGMENT)
public interface GraphXStringVEDFragment<OID_T, VID_T, VD_T, ED_T>
    extends BaseGraphXFragment<OID_T, VID_T, VD_T, ED_T> {

    @FFINameAlias("GetEdataArray") @CXXReference StringTypedArray getEdataArray();

    @FFINameAlias("GetVdataArray") @CXXReference StringTypedArray getVdataArray();
}

