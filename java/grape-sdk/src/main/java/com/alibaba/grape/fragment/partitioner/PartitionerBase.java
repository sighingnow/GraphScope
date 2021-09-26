package com.alibaba.grape.fragment.partitioner;

public interface PartitionerBase<VID_T> {
  public int GetPartitionId(VID_T id);
}
