package com.alibaba.grape.utils;

import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.ds.VertexRange;

import java.util.Arrays;

public class LongArrayWrapper {
  private long data[];
  private int size;
  private int left;
  private int right;

  public LongArrayWrapper() {
    left =0;
    right = 0;
    size = 0;
  }

  public LongArrayWrapper(int s, long defaultValue) {
    size = s;
    left = 0;
    right = s;
    data = new long[s];
    Arrays.fill(data, defaultValue);
  }
  public LongArrayWrapper(VertexRange<Long> vertices, long defaultValue) {
    left = vertices.begin().GetValue().intValue();
    right = vertices.end().GetValue().intValue();
    size = right - left;
    data = new long [size];
    Arrays.fill(data, defaultValue);
  }

  public long get(Vertex<Long> vertex){
    return data[vertex.GetValue().intValue() - left];
  }
  public void set(Vertex<Long> vertex, long newValue){
    data[vertex.GetValue().intValue() - left] = newValue;
  }

  public long get(int ind) {
    return data[ind - left];
  }

  public long get(long ind) {
    return data[(int) ind - left];
  }

  public void set(long ind, long newValue) {
    data[(int) ind - left] = newValue;
  }
  public void set(int ind, long newValue) {
    data[ind - left] = newValue;
  }

  public int getSize() {
    return size;
  }

  public void set(long newValue) {
    for (int i = 0; i < size; ++i) {
      data[i] = newValue;
    }
  }
}