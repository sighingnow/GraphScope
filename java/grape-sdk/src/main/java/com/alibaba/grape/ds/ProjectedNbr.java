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

import static com.alibaba.grape.utils.CppClassName.PROJECTED_NBR;
import static com.alibaba.grape.utils.CppHeaderName.ARROW_PROJECTED_FRAGMENT_H;
import static com.alibaba.grape.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;
import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(ARROW_PROJECTED_FRAGMENT_H)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(PROJECTED_NBR)
@CXXTemplate(cxx = { "uint64_t", "int64_t" }, java = { "java.lang.Long", "java.lang.Long" })
@CXXTemplate(cxx = { "uint64_t", "int32_t" }, java = { "java.lang.Long", "java.lang.Integer" })
@CXXTemplate(cxx = { "uint64_t", "double" }, java = { "java.lang.Long", "java.lang.Double" })
@CXXTemplate(cxx = { "uint64_t", "uint64_t" }, java = { "java.lang.Long", "java.lang.Long" })
@CXXTemplate(cxx = { "uint64_t", "uint32_t" }, java = { "java.lang.Long", "java.lang.Integer" })
public interface ProjectedNbr<VID_T, EDATA_T> extends FFIPointer {
    @CXXValue
    Vertex<VID_T> neighbor();

    @FFINameAlias("edge_id")
    long edgeId();

    EDATA_T data();

    @CXXOperator("++")
    @CXXReference
    ProjectedNbr<VID_T, EDATA_T> inc();

    @CXXOperator("==")
    boolean eq(@CXXReference ProjectedNbr<VID_T, EDATA_T> rhs);

    @CXXOperator("--")
    @CXXReference
    ProjectedNbr<VID_T, EDATA_T> dec();
}
