package com.alibaba.grape.ds;

import com.alibaba.fastffi.CXXPointerRangeElement;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;

import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;

/**
 * A pointer to an unsigned pointer
 */
@FFIGen(library = GRAPE_JNI_LIBRARY)
@FFITypeAlias("unsigned")
public interface FidPointer extends FFIPointer, CXXPointerRangeElement<FidPointer> {
}
