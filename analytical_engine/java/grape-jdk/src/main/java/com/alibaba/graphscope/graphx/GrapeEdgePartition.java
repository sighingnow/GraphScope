package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder;
import com.alibaba.graphscope.ds.ImmutableCSR;
import com.alibaba.graphscope.ds.MutableTypedArray;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen
@CXXHead(CppHeaderName.CORE_JAVA_GRAPHX_EDGE_PARTITION_H)
@CXXHead(CppHeaderName.CORE_JAVA_TYPE_ALIAS_H)
@CXXHead(system = "stdint.h")
@FFITypeAlias(CppClassName.GS_EDGE_PARTITION)
public interface GrapeEdgePartition<OID, VID, ED> extends FFIPointer {

    @FFINameAlias("GetVerticesNum")
    long getVerticesNum();

    @FFINameAlias("GetEdgesNum")
    long getEdgesNum();

    @FFINameAlias("GetInEdges")
    @CXXReference ImmutableCSR<VID, ED> getInEdges();

    @FFINameAlias("GetOutEdges")
    @CXXReference ImmutableCSR<VID, ED> getOutEdges();

    /**
     * get all oids appeared in this edge partition
     *
     * @return
     */
    @FFINameAlias("GetOidArray")
    @CXXReference MutableTypedArray<OID> getOidArray();

    @FFINameAlias("LoadEdges")
    void loadEdges(@CXXReference ArrowArrayBuilder<OID> srcOidBuilder, @CXXReference ArrowArrayBuilder<OID> dstOidBuilder,
        @CXXReference ArrowArrayBuilder<ED> edArrowArrayBuilder);
}
