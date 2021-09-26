package com.alibaba.grape.stdcxx;

import com.alibaba.ffi.*;

import static com.alibaba.grape.utils.CPP_LIBRARY_STRINGS.GRAPE_JNI_LIBRARY;

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
