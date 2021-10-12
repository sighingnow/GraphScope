package com.alibaba.grape.stdcxx;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;

import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@FFITypeAlias("char")
public interface CCharPointer extends FFIPointer {
    @CXXOperator("*&")
    byte toByte();
}
