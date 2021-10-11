package com.alibaba.grape.stdcxx;

import com.alibaba.ffi.CXXOperator;
import com.alibaba.ffi.FFIGen;
import com.alibaba.ffi.FFIPointer;
import com.alibaba.ffi.FFITypeAlias;

import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@FFITypeAlias("char")
public interface CCharPointer extends FFIPointer {
    @CXXOperator("*&")
    byte toByte();
}
