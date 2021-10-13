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

import static com.alibaba.grape.utils.CppClassName.GRAPE_VERTEX;
import static com.alibaba.grape.utils.CppHeaderName.GRAPE_VERTEX_ARRAY_H;
import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;

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
