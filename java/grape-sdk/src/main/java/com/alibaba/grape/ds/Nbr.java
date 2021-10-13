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

import static com.alibaba.grape.utils.CppClassName.GRAPE_NBR;
import static com.alibaba.grape.utils.CppHeaderName.GRAPE_ADJ_LIST_H;
import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(GRAPE_ADJ_LIST_H)
@FFITypeAlias(GRAPE_NBR)
@CXXTemplate(cxx = {"uint64_t", "std::string"},
        java = {"java.lang.Long", "com.alibaba.grape.stdcxx.StdString"})
@CXXTemplate(cxx = {"uint64_t", "jdouble"},
        java = {"Long", "Double"})
@CXXTemplate(cxx = {"uint64_t", "int64_t"},
        java = {"Long", "Long"})
@CXXTemplate(cxx = {"uint64_t", "double"},
        java = {"Long", "Double"})
public interface Nbr<VID, EDATA> extends FFIPointer, CXXPointerRangeElement<Nbr<VID, EDATA>> {
    @CXXOperator("*&")
    @CXXValue Nbr<VID, EDATA> copy();

    @FFIGetter
    @CXXReference Vertex<VID> neighbor();

    @FFIGetter
    @CXXReference EDATA data();

    @FFIFactory
    interface Factory<VID, EDATA> {
        Nbr<VID, EDATA> create();

        Nbr<VID, EDATA> create(VID lid);

        Nbr<VID, EDATA> craete(VID lid, @CXXReference EDATA edata);
    }
}
