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

package com.alibaba.grape.graph.loader;

import com.alibaba.fastffi.FFIVector;
import com.alibaba.grape.stdcxx.StdVector;
import java.io.IOException;

/**
 * @param <OID_T>
 * @param <VDATA_T>
 * @param <EDATA_T>
 */
public interface LoaderBase<OID_T, VDATA_T, EDATA_T> {
    public abstract void loadFragment(FFIVector<FFIVector<OID_T>> vidBuffers,
            FFIVector<FFIVector<VDATA_T>> vdataBuffers, FFIVector<FFIVector<OID_T>> esrcBuffers,
            FFIVector<FFIVector<OID_T>> edstBuffers, FFIVector<FFIVector<EDATA_T>> edataBuffers) throws IOException;
}