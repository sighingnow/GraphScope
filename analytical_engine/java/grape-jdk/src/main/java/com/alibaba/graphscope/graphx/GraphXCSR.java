package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.PropertyNbrUnit;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen(library = "grape-jni")
@CXXHead(CppHeaderName.CORE_JAVA_GRAPHX_GRAPHX_CSR_H)
@CXXHead(CppHeaderName.CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(CppClassName.GS_GRAPHX_CSR)
public interface GraphXCSR<VID_T,ED_T> extends FFIPointer {
    long id();

    @FFINameAlias("GetInDegree")
    long getInDegree(VID_T vid);

    @FFINameAlias("GetOutDegree")
    long getOutDegree(VID_T vid);

    @FFINameAlias("IsIEEmpty")
    boolean isInEdgesEmpty(VID_T vid);

    @FFINameAlias("IsOEEmpty")
    boolean isOutEdgesEmpty(VID_T vid);

    @FFINameAlias("GetIEBegin")
    PropertyNbrUnit<VID_T> getIEBegin(VID_T lid);

    @FFINameAlias("GetIEEnd")
    PropertyNbrUnit<VID_T> getIEEnd(VID_T lid);

    @FFINameAlias("GetOEBegin")
    PropertyNbrUnit<VID_T> getOEBegin(VID_T lid);

    @FFINameAlias("GetOEEnd")
    PropertyNbrUnit<VID_T> getOEEnd(VID_T lid);

    /**
     * Inner vnum
     * @return
     */
    @FFINameAlias("VertexNum")
    VID_T vertexNum();

    @FFINameAlias("GetInEdgesNum")
    long getInEdgesNum();

    @FFINameAlias("GetOutEdgesNum")
    long getOutEdgesNum();
    /**
     *
     * @param begin inclusive
     * @param end exclusive
     * @return
     */
    @FFINameAlias("GetPartialInEdgesNum")
    long getPartialInEdgesNum(VID_T begin, VID_T end);

    @FFINameAlias("GetPartialOutEdgesNum")
    long getPartialOutEdgesNum(VID_T begin, VID_T end);
}
