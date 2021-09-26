package com.alibaba.grape.parallel;

import com.alibaba.ffi.*;
import com.alibaba.grape.ds.Vertex;

import static com.alibaba.grape.utils.CPP_CLASSES_STRINGS.GRAPE_LONG_VERTEX;
import static com.alibaba.grape.utils.CPP_CLASSES_STRINGS.GRAPE_MESSAGE_IN_BUFFER;
import static com.alibaba.grape.utils.CPP_HEADER_STRINGS.*;
import static com.alibaba.grape.utils.CPP_LIBRARY_STRINGS.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@FFITypeAlias(GRAPE_MESSAGE_IN_BUFFER)
@CXXHead({GRAPE_PARALLEL_MESSAGE_IN_BUFFER_H, GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H, ARROW_PROJECTED_FRAGMENT_H})
public interface MessageInBuffer extends FFIPointer {
    @FFIFactory
    interface Factory {
        MessageInBuffer create();
    }

    @FFINameAlias("GetMessage") <FRAG_T, MSG_T>
    boolean getMessage(@CXXReference FRAG_T frag, @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex, @CXXReference MSG_T msg);
}