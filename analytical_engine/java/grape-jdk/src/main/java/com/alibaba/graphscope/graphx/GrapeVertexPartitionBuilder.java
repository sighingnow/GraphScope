package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFIVector;
import com.alibaba.graphscope.stdcxx.StdVector;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen
@CXXHead(CppHeaderName.CORE_JAVA_GRAPHX_VERTEX_PARTITION_H)
@CXXHead(system = "stdint.h")
@FFITypeAlias(CppClassName.GS_VERTEX_PARTITION_BUILDER)
public interface GrapeVertexPartitionBuilder<OID,VID,VD> extends FFIPointer {

    @FFINameAlias("Init")
    void init(VID numVertices, VD defaultValue);

    @FFINameAlias("AddVertex")
    void addVertex(@CXXReference StdVector<OID> oids, int fromPid);

    @FFINameAlias("Build")
    void Build(@CXXReference GrapeVertexPartition<OID,VID,VD> partition);

    @FFIFactory
    interface Factory<OID,VID,VD>{
        GrapeVertexPartitionBuilder<OID,VID,VD> create();
        GrapeVertexPartitionBuilder<OID,VID,VD> create(VD val);
    }
}
