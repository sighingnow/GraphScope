package com.alibaba.grape.fragment.partitioner;

import com.alibaba.fastffi.FFIByteString;
import com.alibaba.grape.utils.CityHash;

public class CityHashPartitioner<T> implements  PartitionerBase<T>{
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
