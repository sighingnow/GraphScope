package com.alibaba.graphscope.utils.array.impl;

import com.alibaba.graphscope.utils.array.PrimitiveArray;

public class DoubleArray implements PrimitiveArray<Double> {
    private double[] values;
    public DoubleArray(int len){
        values = new double[len];
    }

    @Override
    public Double get(int index) {
        return values[index];
    }

    @Override
    public void set(int index, Double value) {
        values[index] = value;
    }

    @Override
    public int size() {
        return values.length;
    }
}
