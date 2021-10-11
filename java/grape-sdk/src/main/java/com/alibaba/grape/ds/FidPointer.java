package com.alibaba.grape.ds;

import com.alibaba.ffi.CXXPointerRangeElement;
import com.alibaba.ffi.FFIGen;
import com.alibaba.ffi.FFIPointer;
import com.alibaba.ffi.FFITypeAlias;

import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;

/**
 * A pointer to an unsigned pointer
 */
@FFIGen(library = GRAPE_JNI_LIBRARY)
@FFITypeAlias("unsigned")
public interface FidPointer extends FFIPointer, CXXPointerRangeElement<FidPointer> {
}
