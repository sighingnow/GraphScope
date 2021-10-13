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

package com.alibaba.grape.app;

import com.alibaba.fastffi.FFIByteString;
import com.alibaba.grape.fragment.ImmutableEdgecutFragment;
import com.alibaba.grape.parallel.ParallelMessageManager;
import com.alibaba.grape.stdcxx.StdVector;

/**
 * @param <OID_T>   original id type
 * @param <VID_T>   vertex id type
 * @param <VDATA_T> vertex data type
 * @param <EDATA_T> edge data type
 */
public interface ParallelContextBase<OID_T, VID_T, VDATA_T, EDATA_T> extends ContextBase<OID_T, VID_T, VDATA_T, EDATA_T> {
    void Init(ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T> frag,
              ParallelMessageManager messageManager, StdVector<FFIByteString> args);

    void Output(ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T> frag);
}
