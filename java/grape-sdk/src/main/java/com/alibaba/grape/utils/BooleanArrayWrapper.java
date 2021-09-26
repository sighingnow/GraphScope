package com.alibaba.grape.utils;

import com.alibaba.grape.ds.Vertex;

public class BooleanArrayWrapper {
  private boolean data[];
  private int size;

  public BooleanArrayWrapper() {}

  public BooleanArrayWrapper(int s, boolean defaultValue) {
    size = s;
    data = new boolean[s];
    for (int i = 0; i < size; ++i) {
      data[i] = defaultValue;
    }
  }
  /**
   * Check if any ele in [start, end] is true
   * @param start
   * @param end
   * @return
   */
  public boolean partialEmpty(int start, int end) {
    if (end >= size) {
      System.err.println("Error: out of bound" + end + "," + size);
    }
    for (int i = start; i <= end; ++i) {
      if (data[i])
        return false;
    }
    return true;
  }
  public boolean partialEmpty(long start, long end) {
    if (end >= size) {
      System.err.println("Error: out of bound" + end + "," + size);
    }
    for (int i = (int) start; i <= end; ++i) {
      if (data[i])
        return false;
    }
    return true;
  }

  public void assign(BooleanArrayWrapper other) {
    if (other.getSize() != size) {
      System.err.println("cannot be assigned since size don't equal");
      return;
    }
    for (int i = 0; i < size; ++i) {
      data[i] = other.get(i);
    }
  }

  public boolean get(int ind) {
    return data[ind];
  }

  public boolean get(long ind) {
    return data[(int) ind];
  }

  public boolean get(Vertex<Long> vertex) {return data[vertex.GetValue().intValue()];}

  public void set(int ind, boolean newValue) {
    data[ind] = newValue;
  }

  public void set(Vertex<Long> vertex, boolean newvalue) {data[vertex.GetValue().intValue()] = newvalue;}

  public void set(long ind, boolean newValue) {
    data[(int) ind] = newValue;
  }

  public int getSize() {
    return size;
  }

  public void set(boolean newValue) {
    for (int i = 0; i < size; ++i) {
      data[i] = newValue;
    }
  }

  public void clear() {
    for (int i = 0; i < size; ++i) {
      data[i] = false;
    }
  }
}