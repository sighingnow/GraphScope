package com.alibaba.graphscope.utils.array.impl;

import com.alibaba.graphscope.utils.array.PrimitiveArray;

public class LongArray implements PrimitiveArray<Long> {
    private long[] values;
    public LongArray(int len){
        values = new long[len];
    }
    @Override
    public Long get(int index) {
        return values[index];
    }

    @Override
    public void set(int index, Long value) {
        values[index] = value;
    }

    @Override
    public int size() {
        return values.length;
    }
}
