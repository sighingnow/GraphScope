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

package io.graphscope.column;

import com.alibaba.fastffi.*;
import com.alibaba.grape.ds.GSVertexArray;
import com.alibaba.grape.ds.Vertex;

import static com.alibaba.grape.utils.CppClassName.*;
import static com.alibaba.grape.utils.CppHeaderName.ARROW_PROJECTED_FRAGMENT_H;
import static com.alibaba.grape.utils.CppHeaderName.GRAPE_TYPES_H;
import static io.graphscope.utils.CppClassName.ARROW_FRAGMENT;
import static io.graphscope.utils.CppClassName.LONG_COLUMN;
import static io.graphscope.utils.CppHeaderName.ARROW_FRAGMENT_H;
import static io.graphscope.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;
import static io.graphscope.utils.JNILibraryName.VINEYARD_JNI_LIBRARY;

@FFIGen(library = VINEYARD_JNI_LIBRARY)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@CXXHead(system = "cstdint")
@CXXHead(GRAPE_TYPES_H)
@CXXHead(ARROW_FRAGMENT_H)
@CXXHead(ARROW_PROJECTED_FRAGMENT_H)
@FFITypeAlias(LONG_COLUMN)
@CXXTemplate(cxx = { ARROW_FRAGMENT + "<int64_t>" }, java = { "io.graphscope.fragment.ArrowFragment<java.lang.Long>" })
@CXXTemplate(cxx = { ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,grape::EmptyType,int64_t>" }, java = {
        "com.alibaba.grape.fragment.ArrowProjectedFragment<Long,Long,com.alibaba.grape.ds.EmptyType,Long>" })
public interface LongColumn<FRAG_T> extends FFIPointer {
    double at(@CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex);

    void set(@CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex, long value);

    @CXXReference
    @FFITypeAlias(GS_VERTEX_ARRAY + "<uint64_t>")
    GSVertexArray<Long> data();
}
