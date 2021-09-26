package com.alibaba.grape.ds;

import com.alibaba.ffi.*;

import static com.alibaba.grape.utils.CPP_CLASSES_STRINGS.GRAPE_EMPTY_TYPE;
import static com.alibaba.grape.utils.CPP_HEADER_STRINGS.GRAPE_TYPES_H;
import static com.alibaba.grape.utils.CPP_LIBRARY_STRINGS.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(GRAPE_TYPES_H)
@FFITypeAlias(GRAPE_EMPTY_TYPE)
public interface EmptyType extends FFIPointer {
    Factory factory = FFITypeFactory.getFactory(Factory.class, EmptyType.class);

    @FFIFactory
    interface Factory {
        EmptyType create();
    }
}
