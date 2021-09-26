package com.alibaba.grape.stdcxx;

import com.alibaba.ffi.CXXHead;
import com.alibaba.ffi.FFIGen;
import com.alibaba.ffi.FFIPointer;
import com.alibaba.ffi.FFITypeAlias;

import static com.alibaba.grape.utils.CPP_HEADER_STRINGS.GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H;
import static com.alibaba.grape.utils.CPP_LIBRARY_STRINGS.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(system = "memory")
@CXXHead(GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H)
@FFITypeAlias("std::shared_ptr")
public interface GrapeStdSharedPtr<T extends FFIPointer> extends FFIPointer {
    //& will return the pointer of T.
    //shall be cxxvalue?
    T get();
}
