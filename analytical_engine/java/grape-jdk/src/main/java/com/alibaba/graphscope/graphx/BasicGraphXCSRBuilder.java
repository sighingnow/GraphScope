package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder;
import com.alibaba.graphscope.stdcxx.StdSharedPtr;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen(library = "grape-jni")
@CXXHead(CppHeaderName.CORE_JAVA_GRAPHX_GRAPHX_CSR_H)
@CXXHead(CppHeaderName.CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(CppClassName.GS_BASIC_GRAPHX_CSR_BUILDER)
public interface BasicGraphXCSRBuilder<OID_T,VID_T,ED_T> extends FFIPointer {
    @FFINameAlias("LoadEdges")
    void loadEdges(@CXXReference ArrowArrayBuilder<OID_T> srcBuilder, @CXXReference ArrowArrayBuilder<OID_T> dstBuilder,
        @CXXReference ArrowArrayBuilder<ED_T> edataBuilder,
        @CXXReference GraphXVertexMap<OID_T,VID_T> graphXVertexMap);

    @FFINameAlias("MySeal")
    @CXXValue StdSharedPtr<GraphXCSR<VID_T,ED_T>> seal(@CXXReference VineyardClient client);

    @FFIFactory
    interface Factory<OID_T,VID_T,ED_T> {
        BasicGraphXCSRBuilder<OID_T,VID_T,ED_T> create(@CXXReference VineyardClient vineyardClient, boolean outEdges);
    }
}
