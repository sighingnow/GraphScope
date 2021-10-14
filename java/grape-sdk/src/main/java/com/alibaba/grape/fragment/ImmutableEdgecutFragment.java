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

package com.alibaba.grape.fragment;

import com.alibaba.fastffi.*;

import static com.alibaba.grape.utils.CppClassName.GRAPE_IMMUTABLE_FRAGMENT;
import static com.alibaba.grape.utils.CppHeaderName.GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H;
import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;

/**
 * This is the base class for auto-generated ImmutableEdgecutFragment.
 *
 * @param <OID_T>
 * @param <VID_T>
 * @param <VDATA_T>
 * @param <EDATA_T>
 */
@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H)
@FFITypeAlias(GRAPE_IMMUTABLE_FRAGMENT)
@CXXTemplate(cxx = { "uint64_t", "uint64_t", "uint64_t",
        "double" }, cxxFull = "uint64_t,uint64_t,uint64_t,double", java = { "Long", "Long", "Long", "Double" })
@CXXTemplate(cxx = { "jlong", "uint64_t", "jlong", "jdouble" }, java = { "Long", "Long", "Long", "Double" })
public interface ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T>
        extends EdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T> {

    int fid_offset();

    VID_T id_mask();

    // @FFINameAlias("GetOutgoingEdgeNum")
    // long GetOutgoingEdgeNum();
    //
    // @FFINameAlias("GetIncomingEdgeNum")
    // long GetIncomingEdgeNum();

    @FFIFactory
    interface Factory<OID_T, VID_T, VDATA_T, EDATA_T> {
        ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T> create();
    }
}
