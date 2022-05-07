package com.alibaba.graphscope.ds;

import static com.alibaba.graphscope.utils.CppClassName.GS_MUTABLE_TYPE_ARRAY;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;

@FFIGen
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@CXXHead(system = "cstdint")
@FFITypeAlias(GS_MUTABLE_TYPE_ARRAY)
public interface MutableTypedArray<T> extends FFIPointer {

    @FFINameAlias("Get")
    T get(long ind);

    @FFINameAlias("Set")
    void set(long ind, T value);

    @FFINameAlias("GetLength")
    long getLength();
}
