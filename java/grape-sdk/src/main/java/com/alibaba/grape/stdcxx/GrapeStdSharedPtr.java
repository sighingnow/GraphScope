package com.alibaba.grape.stdcxx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;

import static com.alibaba.grape.utils.CppHeaderName.GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H;
import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(system = "memory")
@CXXHead(GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H)
@FFITypeAlias("std::shared_ptr")
public interface GrapeStdSharedPtr<T extends FFIPointer> extends FFIPointer {
    //& will return the pointer of T.
    //shall be cxxvalue?
    T get();
}
