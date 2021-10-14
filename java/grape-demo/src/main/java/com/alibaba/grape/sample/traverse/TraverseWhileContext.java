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

package com.alibaba.grape.sample.traverse;

import com.alibaba.fastffi.FFIByteString;
import com.alibaba.grape.app.DefaultContextBase;
import com.alibaba.grape.fragment.ImmutableEdgecutFragment;
import com.alibaba.grape.parallel.DefaultMessageManager;
import com.alibaba.grape.stdcxx.StdVector;

public class TraverseWhileContext implements DefaultContextBase<Long, Long, Long, Double> {

    public int step;
    public int maxStep;
    public long fake_vid;
    public double fake_edata;

    @Override
    public void Init(ImmutableEdgecutFragment<Long, Long, Long, Double> immutableEdgecutFragment,
            DefaultMessageManager javaDefaultMessageManager, StdVector<FFIByteString> args) {
        maxStep = Integer.parseInt(args.get(0).toString());
        step = 0;
    }

    @Override
    public void Output(ImmutableEdgecutFragment<Long, Long, Long, Double> immutableEdgecutFragment) {

    }
}
