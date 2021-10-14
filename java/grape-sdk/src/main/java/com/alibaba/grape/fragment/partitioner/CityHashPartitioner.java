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

package com.alibaba.grape.fragment.partitioner;

import com.alibaba.fastffi.FFIByteString;
import com.alibaba.grape.utils.CityHash;

public class CityHashPartitioner<T> implements PartitionerBase<T> {
    private final int fragNum;

    public CityHashPartitioner(int fnum_) {
        fragNum = fnum_;
    }

    @Override
    public int GetPartitionId(T id) {
        String str = id.toString();
        int res = (int) (CityHash.hash64raw(str.getBytes(), str.getBytes().length) % fragNum);
        return res < 0 ? (res + fragNum) : res;
    }
}
