package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen
@CXXHead(CppHeaderName.CORE_JAVA_GRAPHX_EDGE_PARTITION_H)
@CXXHead(system = "stdint.h")
@FFITypeAlias(CppClassName.GS_EDGE_PARTITION)
public interface GrapeEdgePartition<OID,VID,ED> extends FFIPointer {

    @FFINameAlias("GetVerticesNum")
    long getVerticesNum();

    @FFINameAlias("GetEdgesNum")
    long getEdgesNum();
}
