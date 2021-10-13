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

package com.alibaba.grape.stdcxx;

import com.alibaba.fastffi.*;

import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;


@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(value = {"stdint.h"}, system = {"unordered_map"})
@FFITypeAlias("std::unordered_map")
@CXXTemplate(cxx = {"unsigned", "uint64_t"}, java = {"java.lang.Integer", "java.lang.Long"})
public interface StdUnorderedMap<KEY_T, VALUE_T> extends CXXPointer {
    @FFIFactory
    interface Factory<KEY_T, VALUE_T> {
        StdUnorderedMap<KEY_T, VALUE_T> create();
    }

    int size();

    boolean empty();

    @CXXReference
    @CXXOperator("[]")
    VALUE_T get(@CXXReference KEY_T key);

    @CXXOperator("[]")
    void set(@CXXReference KEY_T key, @CXXReference VALUE_T value);
}
