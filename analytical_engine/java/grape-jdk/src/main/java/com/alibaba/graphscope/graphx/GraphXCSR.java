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

    @FFINameAlias("GetDegree")
    long getDegree(VID_T vid);

    @FFINameAlias("IsEmpty")
    boolean isEmpty(VID_T vid);

    @FFINameAlias("GetBegin")
    PropertyNbrUnit<VID_T> getBegin(VID_T lid);

    @FFINameAlias("GetEnd")
    PropertyNbrUnit<VID_T> getEnd(VID_T lid);

    /**
     * Inner vnum
     * @return
     */
    @FFINameAlias("VertexNum")
    VID_T vertexNum();

    @FFINameAlias("GetTotalEdgesNum")
    long getTotalEdgesNum();

    /**
     *
     * @param begin inclusive
     * @param end exclusive
     * @return
     */
    @FFINameAlias("GetPartialEdgesNum")
    long getPartialEdgesNum(VID_T begin, VID_T end);
}
