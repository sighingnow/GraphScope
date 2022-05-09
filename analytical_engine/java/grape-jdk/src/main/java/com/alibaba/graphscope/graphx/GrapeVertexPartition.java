package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.MutableTypedArray;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen
@CXXHead(CppHeaderName.CORE_JAVA_GRAPHX_VERTEX_PARTITION_H)
@CXXHead(system = "stdint.h")
@FFITypeAlias(CppClassName.GS_VERTEX_PARTITION)
public interface GrapeVertexPartition<OID,VID,VD> extends FFIPointer {

    @FFINameAlias("VerticesNum")
    VID verticesNum();

    @FFINameAlias("Oid2Lid")
    VID oid2Lid(OID oid);

    @FFINameAlias("Lid2Oid")
    OID lid2Oid(VID lid);

    @FFINameAlias("GetVdataArray") @CXXReference MutableTypedArray<VD> getVdataArray();

    @FFINameAlias("GetOidArray") @CXXReference MutableTypedArray<OID> getOidArray();


    @FFIFactory
    interface Factory<OID,VID,VD>{

        GrapeVertexPartition<OID,VID,VD> create();
    }
}
