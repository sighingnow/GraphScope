package com.alibaba.grape.utils;

import com.alibaba.grape.ds.Vertex;
import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.AtomicDoubleArray;

public class AtomicDoubleArrayWrapper {
  private AtomicDoubleArray data;
  private int size;

  public AtomicDoubleArrayWrapper() {}

  public AtomicDoubleArrayWrapper(int s, double defaultValue) {
    size = s;
    data = new AtomicDoubleArray(s);
    for (int i = 0; i < size; ++i) {
      data.set(i, defaultValue);
    }
  }

  public double get(int ind) {
    return data.get(ind);
  }

  public double get(long ind) {
    return data.get((int) ind);
  }
  public double get(Vertex<Long> vertex) {
    return data.get(vertex.GetValue().intValue());
  }

  public void set(int ind, double newValue) {
    data.set(ind, newValue);
  }
  public void set(long ind, double newValue) {
    data.set((int) ind, newValue);
  }
  public void set(Vertex<Long> vertex, double newValue) {
    int lid = vertex.GetValue().intValue();
    data.set(lid, newValue);
  }

  /*
    we want to set the smaller one to ind.
   */
  public void compareAndSetMin(int ind, double newValue) {
    double preValue;
    do {
      preValue = data.get(ind);
    } while (preValue > newValue && !data.compareAndSet(ind, preValue, newValue));
  }

  public void compareAndSetMin(long ind, double newValue) {
    double preValue;
    do {
      preValue = data.get((int) ind);
    } while (preValue > newValue && !data.compareAndSet((int) ind, preValue, newValue));
  }

  public void compareAndSetMin(Vertex<Long> vertex, double newValue) {
    int lid = vertex.GetValue().intValue();
    double preValue;
    do {
      preValue = data.get(lid);
    } while (preValue > newValue && !data.compareAndSet(lid, preValue, newValue));
  }

  public int getSize() {
    return size;
  }

  public void set(double newValue) {
    for (int i = 0; i < size; ++i) {
      data.set(i, newValue);
    }
  }
}