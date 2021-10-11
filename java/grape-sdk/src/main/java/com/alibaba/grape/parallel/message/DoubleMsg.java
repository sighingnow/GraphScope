package com.alibaba.grape.parallel.message;

import com.alibaba.ffi.*;

import static com.alibaba.grape.utils.CppClassName.DOUBLE_MSG;
import static com.alibaba.grape.utils.CppHeaderName.CORE_JAVA_JAVA_MESSAGES_H;
import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;


@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(value = CORE_JAVA_JAVA_MESSAGES_H)
@FFITypeAlias(value = DOUBLE_MSG)
public interface DoubleMsg extends FFIPointer {
    Factory factory = FFITypeFactory.getFactory(Factory.class, DoubleMsg.class);

    @FFIFactory
    interface Factory {
        DoubleMsg create();

        DoubleMsg create(double inData);
    }

    void setData(double value);

    double getData();
}