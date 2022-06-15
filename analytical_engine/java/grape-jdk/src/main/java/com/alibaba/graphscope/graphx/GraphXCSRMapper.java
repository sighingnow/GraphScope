package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder;
import com.alibaba.graphscope.stdcxx.StdSharedPtr;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen(library = "grape-jni")
@CXXHead(CppHeaderName.CORE_JAVA_GRAPHX_GRAPHX_CSR_H)
@CXXHead(CppHeaderName.CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(CppClassName.GS_GRAPHX_CSR_MAPPER)
public interface GraphXCSRMapper<VID_T,OLD_ED_T,NEW_ED_T> extends FFIPointer {

    @FFINameAlias("Map")
    @CXXValue StdSharedPtr<GraphXCSR<VID_T,NEW_ED_T>> map(@CXXReference GraphXCSR<VID_T,OLD_ED_T> oldCsr, @CXXReference ArrowArrayBuilder<NEW_ED_T> array, @CXXReference VineyardClient client);

    @FFIFactory
    interface Factory<VID_T,OLD_ED_T,NEW_ED_T> {
        GraphXCSRMapper<VID_T,OLD_ED_T,NEW_ED_T> create();
    }
}
