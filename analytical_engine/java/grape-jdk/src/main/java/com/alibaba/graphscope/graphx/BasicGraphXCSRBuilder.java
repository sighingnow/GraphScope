package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen(library = "grape-jni")
@CXXHead(CppHeaderName.CORE_JAVA_GRAPHX_GRAPHX_CSR_H)
@CXXHead(CppHeaderName.CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(CppClassName.GS_BASIC_GRAPHX_CSR_BUILDER)
public interface BasicGraphXCSRBuilder<OID_T,VID_T,ED_T> extends FFIPointer {

    @FFIFactory
    interface Factory<OID_T,VID_T,ED_T> {
        BasicGraphXCSRBuilder<OID_T,VID_T,ED_T> create();
    }
}
