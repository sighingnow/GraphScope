package com.alibaba.graphscope.fragment;

import static com.alibaba.graphscope.utils.CppClassName.ARROW_PROJECTED_FRAGMENT_MAPPER;
import static com.alibaba.graphscope.utils.CppHeaderName.ARROW_PROJECTED_FRAGMENT_MAPPER_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder;
import com.alibaba.graphscope.graphx.VineyardClient;
import com.alibaba.graphscope.stdcxx.StdSharedPtr;

@FFIGen
@CXXHead(ARROW_PROJECTED_FRAGMENT_MAPPER_H)
@FFITypeAlias(ARROW_PROJECTED_FRAGMENT_MAPPER)
public interface ArrowProjectedFragmentMapper<OID_T, VID_T, OLD_V_T, NEW_V_T, OLD_E_T, NEW_E_T> extends
    FFIPointer {

    @FFINameAlias("Map")
    @CXXValue StdSharedPtr<ArrowProjectedFragment<OID_T, VID_T, NEW_V_T, NEW_E_T>> map(
        @CXXReference ArrowProjectedFragment<OID_T, VID_T, OLD_V_T, OLD_E_T> oldFrag,
        @CXXReference ArrowArrayBuilder<NEW_V_T> vdBuilder,
        @CXXReference ArrowArrayBuilder<NEW_E_T> edBuilder,
        @CXXReference VineyardClient client);

    @FFIFactory
    interface Factory<OID_T, VID_T, OLD_V_T, NEW_V_T, OLD_E_T, NEW_E_T> {

        ArrowProjectedFragmentMapper<OID_T, VID_T, OLD_V_T, NEW_V_T, OLD_E_T, NEW_E_T> create();
    }
}
