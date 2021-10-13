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

import static com.alibaba.grape.utils.CppClassName.*;
import static com.alibaba.grape.utils.CppHeaderName.*;
import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;


@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(GS_CORE_CONFIG_H)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@CXXHead(GRAPE_VERTEX_ARRAY_H)
@CXXHead(system = "cstdint")
@FFITypeAlias(GS_VERTEX_ARRAY)
@CXXTemplate(cxx = {"double"}, java = {"java.lang.Double"})
@CXXTemplate(cxx = {"uint64_t"}, java = {"java.lang.Long"})
@CXXTemplate(cxx = {"int64_t"}, java = {"java.lang.Long"})
@CXXTemplate(cxx = {"uint32_t"}, java = {"java.lang.Integer"})
@CXXTemplate(cxx = {"int32_t"}, java = {"java.lang.Integer"})
public interface GSVertexArray<T> extends FFIPointer, CXXPointer {
    @FFIFactory
    interface Factory<T> {
        GSVertexArray<T> create();
    }

    void Init(@CXXReference @FFITypeAlias(GRAPE_VERTEX_RANGE + "<uint64_t>") VertexRange<Long> range);

    void Init(@CXXReference @FFITypeAlias(GRAPE_VERTEX_RANGE + "<uint64_t>") VertexRange<Long> range, @CXXReference T val);

    void SetValue(@CXXReference T val);

    void SetValue(@CXXReference @FFITypeAlias(GRAPE_VERTEX_RANGE + "<uint64_t>") VertexRange<Long> range, @CXXReference T val);

    @FFINameAlias("SetValue")
    void set(@CXXReference @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") Vertex<Long> range, @CXXReference T val);

    @FFINameAlias("GetValue")
    @CXXOperator("[]")
    @CXXReference T get(@CXXReference @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") Vertex<Long> v);

    @CXXReference @FFITypeAlias(GRAPE_VERTEX_RANGE + "<uint64_t>") VertexRange<Long> GetVertexRange();
    // void Clear();
}

