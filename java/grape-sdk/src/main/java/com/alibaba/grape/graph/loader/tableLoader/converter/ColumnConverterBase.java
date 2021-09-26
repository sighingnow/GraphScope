package com.alibaba.grape.graph.loader.tableLoader.converter;

import com.alibaba.grape.graph.loader.tableLoader.SQLRecord;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.io.Writable;

public interface ColumnConverterBase {
  public void arrayRecordToSQLRecord(int index, ArrayRecord recIn, SQLRecord recOut);

  public void writableToArrayRecord(int index, Writable valIn, ArrayRecord recOut);

  public void stringToArrayRecord(int index, String valIn, ArrayRecord recOut);
}
