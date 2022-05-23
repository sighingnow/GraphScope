package com.alibaba.graphscope.utils.array.impl;

import com.alibaba.graphscope.utils.array.PrimitiveArray;

public class ObjectArray<T> implements PrimitiveArray<T> {
    private Object[] array;
    private Class<?extends T> clz;
    public ObjectArray(Class<? extends T> clz, long len){
        array = new Object[(int) len];
        this.clz = clz;
    }

    @Override
    public T get(int index) {
        return (T) array[index];
    }

    @Override
    public void set(int index, T value) {
        array[index] = value;
    }

    @Override
    public int size() {
        return array.length;
    }
}
