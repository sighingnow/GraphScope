/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.grape.ds;

import com.alibaba.fastffi.*;

import static com.alibaba.grape.utils.CppClassName.GRAPE_DENSE_VERTEX_SET;
import static com.alibaba.grape.utils.CppHeaderName.GRAPE_DENSE_VERTEX_SET_H;
import static com.alibaba.grape.utils.CppHeaderName.GRAPE_WORKER_COMM_SPEC_H;
import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(value =  {GRAPE_WORKER_COMM_SPEC_H, GRAPE_DENSE_VERTEX_SET_H})
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
