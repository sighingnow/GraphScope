package com.alibaba.grape.ds;

import com.alibaba.ffi.*;

import static com.alibaba.grape.utils.CPP_CLASSES_STRINGS.GRAPE_VERTEX;
import static com.alibaba.grape.utils.CPP_HEADER_STRINGS.GRAPE_VERTEX_ARRAY_H;
import static com.alibaba.grape.utils.CPP_LIBRARY_STRINGS.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(GRAPE_VERTEX_ARRAY_H)
@FFITypeAlias(GRAPE_VERTEX)
@CXXTemplate(cxx = {"uint32_t"}, java = {"Integer"})
@CXXTemplate(cxx = {"uint64_t"}, java = {"Long"})
public interface Vertex<VID_T> extends FFIPointer, CXXPointer, CXXValueRangeElement<Vertex<VID_T>> {
    @FFIFactory
    interface Factory<VID_T> {
        Vertex<VID_T> create();
    }

    /**
     * @return
     */
    @CXXOperator("*&")
    @CXXValue Vertex<VID_T> copy();

    /**
     * Note this is not necessary to be a prefix increment
     *
     * @return
     */
    @CXXOperator("++")
    @CXXReference Vertex<VID_T> inc();

    /**
     * @return
     */
    @CXXOperator("==")
    boolean eq(@CXXReference Vertex<VID_T> t);

    VID_T GetValue();

    void SetValue(VID_T id);
}
