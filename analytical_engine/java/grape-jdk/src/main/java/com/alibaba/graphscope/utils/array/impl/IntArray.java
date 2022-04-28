package com.alibaba.graphscope.utils.array.impl;

import com.alibaba.graphscope.utils.array.PrimitiveArray;

public class IntArray implements PrimitiveArray<Integer> {
    private int[] values;
    public IntArray(int len){
        values = new int[len];
    }

    @Override
    public Integer get(int index) {
        return values[index];
    }

    @Override
    public void set(int index, Integer value) {
        values[index] = value;
    }

    @Override
    public int size() {
        return values.length;
    }
}
