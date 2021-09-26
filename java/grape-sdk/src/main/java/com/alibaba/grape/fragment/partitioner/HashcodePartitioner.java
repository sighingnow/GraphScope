package com.alibaba.grape.fragment.partitioner;

import java.util.Objects;

public class HashcodePartitioner<T> implements PartitionerBase<T> {
  private final int fragNum;
  public HashcodePartitioner(int fnum_) {
    fragNum = fnum_;
  }
  @Override
  public int GetPartitionId(T id) {
    int res = id.hashCode();
    return (res % fragNum) < 0 ? (res % fragNum + fragNum) : res % fragNum;
  }
}