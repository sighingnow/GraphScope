package com.alibaba.graphscope.utils.array.impl;

import com.alibaba.graphscope.ds.ImmutableTypedArray;
import com.alibaba.graphscope.utils.array.PrimitiveArray;

/**
 * Provide array interface with immutable typed array.
 */
public class TypedBackendPrimitiveArray<T> implements PrimitiveArray<T> {
    private ImmutableTypedArray<T> backend;
    public TypedBackendPrimitiveArray(ImmutableTypedArray<T> backend){
        this.backend = backend;
    }

    @Override
    public T get(int index) {
        return backend.get(index);
    }

    @Override
    public void set(int index, T value) {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public int size() {
        return (int) backend.getLength();
    }
}
