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

import static com.alibaba.grape.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;
import static io.graphscope.utils.CppClassName.I_COLUMN;
import static io.graphscope.utils.JNILibraryName.VINEYARD_JNI_LIBRARY;

@FFIGen(library = VINEYARD_JNI_LIBRARY)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(I_COLUMN)
public interface IColumn extends FFIPointer {
    @CXXValue
    FFIByteString name();

    @FFINameAlias("set_name")
    void setName(@CXXReference FFIByteString name);
}
