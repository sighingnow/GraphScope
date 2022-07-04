package com.alibaba.graphscope.ds;

import static com.alibaba.graphscope.utils.CppClassName.GS_DEFAULT_IMMUTABLE_CSR;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISerializable;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.adaptor.Nbr;

@FFIGen
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@CXXHead(system = "cstdint")
@FFITypeAlias(GS_DEFAULT_IMMUTABLE_CSR)
public interface ImmutableCSR<VID_T, ED> extends FFISerializable { //grape::Nbr

    //FIXME: implement this is immutableCSR C++
    default long partialEdgeNum(VID_T start, VID_T end){
        GrapeNbr<VID_T,ED> startNbr = getBegin(start);
        GrapeNbr<VID_T,ED> endNbr = getEnd(end);
        return (endNbr.getAddress() - startNbr.getAddress()) / 8; // the size of pointer
    }

    @FFINameAlias("vertex_num")
    VID_T vertexNum();

    @FFINameAlias("edge_num")
    long edgeNum();

    @FFINameAlias("degree")
    int degree(VID_T lid);

    @FFINameAlias("is_empty")
    boolean isEmpty(VID_T lid);

    @FFINameAlias("get_begin")
    GrapeNbr<VID_T,ED> getBegin(VID_T lid);

    @FFINameAlias("get_end")
    GrapeNbr<VID_T,ED> getEnd(VID_T lid);
}
