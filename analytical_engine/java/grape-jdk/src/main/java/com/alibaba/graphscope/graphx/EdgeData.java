package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISerializable;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.ImmutableTypedArray;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen(library = "grape-jni")
@CXXHead(CppHeaderName.CORE_JAVA_GRAPHX_EDGE_DATA_H)
@FFITypeAlias(CppClassName.GS_EDGE_DATA)
public interface EdgeData<VID,ED> extends FFISerializable {
    long id();
    /**
     * Could contain outer vertices data
     * @return nums
     */
    @FFINameAlias("GetEdgeNum") long getEdgeNum();

    @FFINameAlias("GetEdgeDataByEid") ED getEdgeDataByEid(long eid);

    @FFINameAlias("GetEdataArray") @CXXReference ImmutableTypedArray<ED> getEdataArray();
}
