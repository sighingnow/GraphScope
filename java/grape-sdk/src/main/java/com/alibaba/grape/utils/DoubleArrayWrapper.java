package com.alibaba.grape.utils;

import com.alibaba.grape.ds.Vertex;
import com.google.common.util.concurrent.AtomicDouble;

public class DoubleArrayWrapper {
  private double data[];
  private int size;

  public DoubleArrayWrapper() {}

  public DoubleArrayWrapper(int s, double defaultValue) {
    size = s;
    data = new double[s];
    for (int i = 0; i < size; ++i) {
      data[i] = defaultValue;
    }
  }

  public double get(int ind) {
    return data[ind];
  }

  public double get(long ind) {
    return data[(int) ind];
  }
  public double get(Vertex<Long> vertex) {
    return data[vertex.GetValue().intValue()];
  }

  public void set(int ind, double newValue) {
    data[ind] = newValue;
  }

  public void set(long ind, double newValue) {
    data[(int) ind] = newValue;
  }
  public void set(Vertex<Long> vertex, AtomicDouble adouble) {
    data[vertex.GetValue().intValue()] = adouble.get();
  }

  public void set(Vertex<Long> vertex, double newValue) {
    data[vertex.GetValue().intValue()] = newValue;
  }

  public int getSize() {
    return size;
  }

  public void set(double newValue) {
    for (int i = 0; i < size; ++i) {
      data[i] = newValue;
    }
  }
}