package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen(library = "grape-jni")
@CXXHead(CppHeaderName.CORE_JAVA_GRAPHX_GRAPHX_VERTEX_MAP_H)
@CXXHead(CppHeaderName.CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(CppClassName.GS_GRAPHX_GRAPHX_VERTEX_MAP)
public interface GraphXVertexMap<OID_T,VID_T> extends FFIPointer {
    long id();
    int fid();
    int fnum();

    @FFINameAlias("GetTotalVertexSize")
    long getTotalVertexSize();

    @FFINameAlias("GetVertexSize")
    long getVertexSize();

    @FFINameAlias("GetInnerVertexSize")
    long getInnerVertexSize(int fid);

    default long innerVertexSize(){
        return getInnerVertexSize(fid());
    }
}
