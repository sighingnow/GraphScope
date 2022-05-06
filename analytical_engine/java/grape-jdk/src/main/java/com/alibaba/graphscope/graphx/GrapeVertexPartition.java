package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen
@CXXHead(CppHeaderName.CORE_JAVA_GRAPHX_VERTEX_PARTITION_H)
@CXXHead(system = "stdint.h")
@FFITypeAlias(CppClassName.GS_VERTEX_PARTITION)
public interface GrapeVertexPartition extends FFIPointer {

}
