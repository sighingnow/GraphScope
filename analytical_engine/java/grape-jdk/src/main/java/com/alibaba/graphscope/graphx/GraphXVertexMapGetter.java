package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.stdcxx.StdSharedPtr;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen(library = "grape-jni")
@CXXHead(CppHeaderName.CORE_JAVA_GRAPHX_GRAPHX_VERTEX_MAP_H)
@CXXHead(CppHeaderName.CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(CppClassName.GS_GRAPHX_GRAPHX_VERTEX_MAP_GETTER)
public interface GraphXVertexMapGetter<OID_T,VID_T> extends FFIPointer {
    @FFINameAlias("Get")
    StdSharedPtr<GraphXVertexMap<OID_T,VID_T>> get(@CXXReference VineyardClient client, long globalVMID);

    @FFIFactory
    interface Factory<OID_T,VID_T>{
        GraphXVertexMapGetter<OID_T,VID_T> create();
    }
}
