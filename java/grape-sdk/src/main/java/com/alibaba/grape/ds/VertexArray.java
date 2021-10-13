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

import static com.alibaba.grape.utils.CppClassName.GRAPE_VERTEX_ARRAY;
import static com.alibaba.grape.utils.CppHeaderName.GRAPE_VERTEX_ARRAY_H;
import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(GRAPE_VERTEX_ARRAY_H)
@FFITypeAlias(GRAPE_VERTEX_ARRAY)
@CXXTemplate(cxx = {"jboolean", "uint32_t"}, java = {"java.lang.Boolean", "java.lang.Integer"})
@CXXTemplate(cxx = {"jdouble", "uint32_t"}, java = {"java.lang.Double", "java.lang.Integer"})
@CXXTemplate(cxx = {"jdouble", "uint64_t"}, java = {"java.lang.Double", "java.lang.Long"})
@CXXTemplate(cxx = {"double", "uint64_t"}, java = {"java.lang.Double", "java.lang.Long"})
@CXXTemplate(cxx = {"jlong", "uint64_t"}, java = {"java.lang.Long", "java.lang.Long"})
public interface VertexArray<T, VID> extends FFIPointer, CXXPointer {
    @FFIFactory
    interface Factory<T, VID> {
        VertexArray<T, VID> create();
    }

    void Init(@CXXReference VertexRange<VID> range);

    void Init(@CXXReference VertexRange<VID> range, @CXXReference T val);

    void SetValue(@CXXReference T val);

    void SetValue(@CXXReference VertexRange<VID> range, @CXXReference T val);

    @FFINameAlias("SetValue")
    void set(@CXXReference Vertex<VID> range, @CXXReference T val);

    @FFINameAlias("GetValue")
    @CXXOperator("[]")
    @CXXReference T get(@CXXReference Vertex<VID> v);

    @CXXReference VertexRange<VID> GetVertexRange();
    // void Clear();
}
