package com.alibaba.graphscope.ds;

import static com.alibaba.graphscope.utils.CppClassName.GS_DEFAULT_IMMUTABLE_CSR;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;

@FFIGen
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@CXXHead(system = "cstdint")
@FFITypeAlias(GS_DEFAULT_IMMUTABLE_CSR)
public interface ImmutableCSR<VID_T, ED> extends FFIPointer { //grape::Nbr

    @FFINameAlias("vertex_num")
    VID_T vertexNum();

    @FFINameAlias("edge_num")
    long edgeNum();

    @FFINameAlias("degree")
    int degree(VID_T lid);

    @FFINameAlias("is_empty")
    boolean isEmpty(VID_T lid);

    @FFINameAlias("get_begin")
    @CXXReference GrapeNbr<VID_T,ED> getBegin(VID_T lid);

    @FFINameAlias("get_end")
    @CXXReference GrapeNbr<VID_T,ED> getEnd(VID_T lid);
}
