package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen(library = "grape-jni")
@CXXHead(CppHeaderName.CORE_JAVA_GRAPHX_LOCAL_VERTEX_MAP_H)
@CXXHead(CppHeaderName.CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(CppClassName.GS_GRAPHX_LOCAL_VERTEX_MAP)
public interface BasicLocalVertexMapBuilder<OID_T,VID_T> extends FFIPointer {

    @FFINameAlias("Seal")
    LocalVertexMap<OID_T,VID_T> seal(@CXXReference VineyardClient client);

    @FFIFactory
    interface Factory<OID_T,VID_T>{
        BasicLocalVertexMapBuilder<OID_T,VID_T> create(@CXXReference VineyardClient client, @CXXReference ArrowArrayBuilder<OID_T> innerOids);
    }
}
