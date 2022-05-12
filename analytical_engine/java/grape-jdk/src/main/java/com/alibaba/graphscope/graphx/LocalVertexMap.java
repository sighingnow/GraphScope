package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen(library = "grape-jni")
@CXXHead(CppHeaderName.CORE_JAVA_GRAPHX_LOCAL_VERTEX_MAP_H)
@CXXHead(CppHeaderName.CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(CppClassName.GS_GRAPHX_LOCAL_VERTEX_MAP)
public interface LocalVertexMap<OID_T,VID_T> extends FFIPointer {
    long id();

    @FFINameAlias("GetInnerVerticesNum")
    long getInnerVerticesNum();
}
