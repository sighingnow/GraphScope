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

import java.util.Iterator;

import static com.alibaba.grape.utils.CppClassName.PROJECTED_ADJ_LIST;
import static com.alibaba.grape.utils.CppHeaderName.ARROW_PROJECTED_FRAGMENT_H;
import static com.alibaba.grape.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;
import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(ARROW_PROJECTED_FRAGMENT_H)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(PROJECTED_ADJ_LIST)
@CXXTemplate(cxx = { "uint64_t", "int64_t" }, java = { "java.lang.Long", "java.lang.Long" })
@CXXTemplate(cxx = { "uint64_t", "int32_t" }, java = { "java.lang.Long", "java.lang.Integer" })
@CXXTemplate(cxx = { "uint64_t", "double" }, java = { "java.lang.Long", "java.lang.Double" })
@CXXTemplate(cxx = { "uint64_t", "uint64_t" }, java = { "java.lang.Long", "java.lang.Long" })
@CXXTemplate(cxx = { "uint64_t", "uint32_t" }, java = { "java.lang.Long", "java.lang.Integer" })
public interface ProjectedAdjList<VID_T, EDATA_T> extends FFIPointer {
    @CXXValue
    ProjectedNbr<VID_T, EDATA_T> begin();

    @CXXValue
    ProjectedNbr<VID_T, EDATA_T> end();

    @FFINameAlias("Size")
    long size();

    @FFINameAlias("Empty")
    boolean empty();

    @FFINameAlias("NotEmpty")
    boolean notEmpty();

    default Iterable<ProjectedNbr<VID_T, EDATA_T>> iterator() {
        return () -> new Iterator<ProjectedNbr<VID_T, EDATA_T>>() {
            ProjectedNbr<VID_T, EDATA_T> cur = begin().dec();
            ProjectedNbr<VID_T, EDATA_T> end = end();
            boolean flag = false;

            @Override
            public boolean hasNext() {
                if (!flag) {
                    cur = cur.inc();
                    flag = !cur.eq(end);
                }
                return flag;
            }

            @Override
            public ProjectedNbr<VID_T, EDATA_T> next() {
                flag = false;
                return cur;
            }
        };
    }
}
