package com.alibaba.grape.graph.loader.tableLoader.converter;

import com.alibaba.grape.graph.loader.tableLoader.*;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Writable;

public class BigIntConverter implements ColumnConverterBase {
  private LongWritable value;

  public BigIntConverter() {
    value = new LongWritable();
  }

  @Override
  public void arrayRecordToSQLRecord(int index, ArrayRecord recIn, SQLRecord recOut) {
    value.set(recIn.getBigint(index));
    recOut.set(index, value);
  }

  @Override
  public void writableToArrayRecord(int index, Writable valIn, ArrayRecord recOut) {
    recOut.set(index, ((LongWritable) valIn).get());
  }

  @Override
  public void stringToArrayRecord(int index, String valIn, ArrayRecord recOut) {
    recOut.set(index, Long.valueOf(valIn));
  }
}
