package com.alibaba.grape.stdcxx;

import com.alibaba.ffi.*;

import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@FFITypeAlias("double")
public interface DoublePointer extends FFIPointer, CXXPointer {
    Factory factory = FFITypeFactory.getFactory(Factory.class, DoublePointer.class);

    @FFIFactory
    interface Factory {
        DoublePointer create();
    }

    @CXXOperator("*&")
    double toDouble();

    @CXXOperator("=")
    void SetValue(double value);
}
