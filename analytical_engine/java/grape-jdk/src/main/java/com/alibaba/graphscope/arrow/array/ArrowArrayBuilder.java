package com.alibaba.graphscope.arrow.array;

import static com.alibaba.graphscope.utils.CppClassName.GS_ARROW_ARRAY_BUILDER;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISerializable;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.arrow.Status;

@FFIGen(library = "grape-jni")
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(GS_ARROW_ARRAY_BUILDER)
public interface ArrowArrayBuilder<T> extends FFISerializable {

    @FFINameAlias("Reserve")
    @CXXValue Status reserve(long additionalCapacity);

    @FFINameAlias("UnsafeAppend")
    void unsafeAppend(T value);

    @FFINameAlias("GetValue")
    long getValue(long index);

    @FFIFactory
    interface Factory<T> {

        ArrowArrayBuilder<T> create();
    }
}
