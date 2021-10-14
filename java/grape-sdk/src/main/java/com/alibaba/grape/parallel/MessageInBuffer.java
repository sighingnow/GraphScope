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

package com.alibaba.grape.parallel;

import com.alibaba.fastffi.*;
import com.alibaba.grape.ds.Vertex;

import static com.alibaba.grape.utils.CppClassName.GRAPE_LONG_VERTEX;
import static com.alibaba.grape.utils.CppClassName.GRAPE_MESSAGE_IN_BUFFER;
import static com.alibaba.grape.utils.CppHeaderName.*;
import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@FFITypeAlias(GRAPE_MESSAGE_IN_BUFFER)
@CXXHead({ GRAPE_PARALLEL_MESSAGE_IN_BUFFER_H, GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H,
        ARROW_PROJECTED_FRAGMENT_H })
public interface MessageInBuffer extends FFIPointer {
    @FFIFactory
    interface Factory {
        MessageInBuffer create();
    }

    @FFINameAlias("GetMessage")
    <FRAG_T, MSG_T> boolean getMessage(@CXXReference FRAG_T frag,
            @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex, @CXXReference MSG_T msg);
}