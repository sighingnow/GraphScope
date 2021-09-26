package com.alibaba.grape.ds;

import com.alibaba.ffi.*;

import static com.alibaba.grape.utils.CPP_CLASSES_STRINGS.GRAPE_DENSE_VERTEX_SET;
import static com.alibaba.grape.utils.CPP_HEADER_STRINGS.GRAPE_DENSE_VERTEX_SET_H;
import static com.alibaba.grape.utils.CPP_HEADER_STRINGS.GRAPE_WORKER_COMM_SPEC_H;
import static com.alibaba.grape.utils.CPP_LIBRARY_STRINGS.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(value = {"aaa.h", GRAPE_WORKER_COMM_SPEC_H, GRAPE_DENSE_VERTEX_SET_H})
@FFITypeAlias(GRAPE_DENSE_VERTEX_SET)
@CXXTemplate(cxx = {"uint32_t"}, java = {"java.lang.Integer"})
@CXXTemplate(cxx = {"uint64_t"}, java = {"java.lang.Long"})
public interface DenseVertexSet<VID_T> extends FFIPointer, CXXPointer {
    @FFIFactory
    interface Factory<VID_T> {
        DenseVertexSet<VID_T> create();
    }

    //default void Init(@CXXReference VertexRange<VID_T> range) {
//        Init(range, 1);
//    }

    void Init(@CXXReference VertexRange<VID_T> range);

    void Insert(@CXXReference Vertex<VID_T> u);

    boolean InsertWithRet(@CXXReference Vertex<VID_T> u);

    void Erase(@CXXReference Vertex<VID_T> u);

    boolean EraseWithRet(@CXXReference Vertex<VID_T> u);

    boolean Exist(@CXXReference Vertex<VID_T> u);

    @CXXValue VertexRange<VID_T> Range();

    long Count();

//    long ParallelCount(int thread_num);

    long PartialCount(VID_T beg, VID_T end);

//    long ParallelPartialCount(int thread_num, VID_T beg, VID_T end);

    void Clear();

//    void ParallelClear(int thread_num);

    void Swap(@CXXReference DenseVertexSet<VID_T> rhs);

    @CXXReference Bitset GetBitset();

    boolean Empty();

    boolean PartialEmpty(VID_T beg, VID_T end);
}
