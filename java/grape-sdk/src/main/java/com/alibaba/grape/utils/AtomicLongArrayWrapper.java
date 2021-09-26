package com.alibaba.grape.utils;

import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.ds.VertexRange;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLongArray;

public class AtomicLongArrayWrapper {
    private AtomicLongArray data;
    private int size;
    private int left;
    private int right;


    public AtomicLongArrayWrapper(int s, long defaultValue) {
        size = s;
        left = 0;
        right = s;
        long tmp [] = new long[s];
        Arrays.fill(tmp, defaultValue);
        data = new AtomicLongArray(tmp);
    }
    public AtomicLongArrayWrapper(VertexRange<Long> vertices, long defaultValue) {
        left = vertices.begin().GetValue().intValue();
        right = vertices.end().GetValue().intValue();
        size = right - left;
        long tmp [] = new long[size];
        Arrays.fill(tmp, defaultValue);
        data = new AtomicLongArray(tmp);
    }

    public long get(int ind) {
        return data.get(ind - left);
    }

    public long get(long ind) {
        return data.get((int) ind - left);
    }
    public long get(Vertex<Long> vertex) {
        return data.get(vertex.GetValue().intValue() -left );
    }

    public void set(int ind, long newValue) {
        data.set(ind -left , newValue);
    }
    public void set(long ind, long newValue) {
        data.set((int) ind -left , newValue);
    }
    public void set(Vertex<Long> vertex, long newValue) {
        int lid = vertex.GetValue().intValue();
        data.set(lid - left, newValue);
    }

    /*
      we want to set the smaller one to ind.
     */
    public void compareAndSetMin(int ind, long newValue) {
        long preValue;
        do {
            preValue = data.get(ind -left );
        } while (preValue > newValue && !data.compareAndSet(ind -left, preValue, newValue));
    }

    public void compareAndSetMin(long ind, long newValue) {
        long preValue;
        do {
            preValue = data.get((int) ind -left);
        } while (preValue > newValue && !data.compareAndSet((int) ind -left, preValue, newValue));
    }

    public void compareAndSetMin(Vertex<Long> vertex, long newValue) {
        int lid = vertex.GetValue().intValue();
        long preValue;
        do {
            preValue = data.get(lid -left );
        } while (preValue > newValue && !data.compareAndSet(lid -left, preValue, newValue));
    }

    /**
     * Atomicl update the array, compare values using unsigned comparasion.
     * @param vertex
     * @param newValue
     */
    public void compareAndSetMinUnsigned(Vertex<Long> vertex, long newValue) {
        int lid = vertex.GetValue().intValue();
        long preValue;
        do {
            preValue = data.get(lid -left );
        } while (Long.compareUnsigned(preValue, newValue) > 0 && !data.compareAndSet(lid -left, preValue, newValue));
    }

    public int getSize() {
        return size;
    }

    public void set(long newValue) {
        for (int i = 0; i < size; ++i) {
            data.set(i, newValue);
        }
    }
}
