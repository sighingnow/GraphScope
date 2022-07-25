package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.stdcxx.StdVector;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen(library = "grape-jni")
@CXXHead(CppHeaderName.VINEYARD_ARRAY_BUILDER_H)
@FFITypeAlias(CppClassName.VINEYARD_ARRAY_BUILDER)
public interface VineyardArrayBuilder<EDATA_T> extends FFIPointer {
    @CXXOperator("[]")
    @CXXReference
    EDATA_T get(long index);

    @CXXOperator("[]")
    void set(long index, EDATA_T value);

    long size();

    @FFIFactory
    interface Factory<EDATA_T> {

        VineyardArrayBuilder<EDATA_T> create(@CXXReference VineyardClient client,
            @CXXReference StdVector<EDATA_T> newValues);
        VineyardArrayBuilder<EDATA_T> create(@CXXReference VineyardClient client,
            long size);
    }
}
