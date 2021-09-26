package com.alibaba.grape.graph.loader.tableLoader.converter;

import com.alibaba.grape.graph.loader.tableLoader.SQLRecord;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.io.DoubleWritable;
import com.aliyun.odps.io.Writable;

public class DoubleConverter implements ColumnConverterBase {
  private DoubleWritable value;

  public DoubleConverter() {
    value = new DoubleWritable();
  }

  @Override
  public void arrayRecordToSQLRecord(int index, ArrayRecord recIn, SQLRecord recOut) {
    value.set(recIn.getDouble(index));
    recOut.set(index, value);
  }

  @Override
  public void writableToArrayRecord(int index, Writable valIn, ArrayRecord recOut) {
    recOut.set(index, ((DoubleWritable) valIn).get());
  }
  @Override
  public void stringToArrayRecord(int index, String valIn, ArrayRecord recOut) {
    recOut.set(index, Double.valueOf(valIn));
  }
}