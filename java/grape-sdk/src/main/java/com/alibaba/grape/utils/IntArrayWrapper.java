package com.alibaba.grape.utils;

import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.ds.VertexRange;
import org.apache.arrow.flatbuf.Int;

import java.util.Arrays;

public class IntArrayWrapper {
  private int data[];
  private int left;
  private int right;
  private int size;

  public IntArrayWrapper() {
    left =0;
    right =0;
    size = 0;
  }

  public IntArrayWrapper(int s, int defaultValue) {
    size = s;
    left = 0;
    right = s;
    data = new int[s];
    Arrays.fill(data, defaultValue);
  }
  public IntArrayWrapper(VertexRange<Long> vertices, int defaultValue){
    left = vertices.begin().GetValue().intValue();
    right = vertices.end().GetValue().intValue();
    size = right - left;
    data = new int [size];
    Arrays.fill(data, defaultValue);
  }

  public int get(int ind) {
    return data[ind - left];
  }

  public int get(long ind) {
    return data[(int) ind - left];
  }
  public int get(Vertex<Long> vertex){return data[vertex.GetValue().intValue() - left];}

  public void set(long ind, int newValue) {
    data[(int) ind -left] = newValue;
  }

  public void set(int ind, int newValue) {
    data[ind - left] = newValue;
  }
  public void set(Vertex<Long> vertex, int newValue){data[vertex.GetValue().intValue() - left] = newValue;}

  public int getSize() {
    return size;
  }

  public void set(int newValue) {
    for (int i = 0; i < size; ++i) {
      data[i] = newValue;
    }
  }
}
