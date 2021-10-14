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

package io.graphscope.ds;

import com.alibaba.fastffi.*;
import com.alibaba.grape.ds.Vertex;
import io.graphscope.utils.CppClassName;
import io.graphscope.utils.CppHeaderName;
import io.graphscope.utils.JNILibraryName;

@FFIGen(library = JNILibraryName.VINEYARD_JNI_LIBRARY)
@CXXHead(CppHeaderName.CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(CppClassName.PROPERTY_NBR_UNIT)
@CXXTemplate(cxx = { "uint64_t" }, java = { "java.lang.Long" })
public interface PropertyNbrUnit<VID_T> extends FFIPointer, CXXPointerRangeElement<PropertyNbrUnit<VID_T>> {
    @FFIGetter
    VID_T vid();

    @FFIGetter
    long eid();

    @FFINameAlias("get_neighbor")
    @CXXValue
    Vertex<VID_T> getNeighbor();
}
