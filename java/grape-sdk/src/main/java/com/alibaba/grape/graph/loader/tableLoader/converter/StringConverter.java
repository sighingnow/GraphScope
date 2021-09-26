package com.alibaba.grape.graph.loader.tableLoader.converter;

import com.alibaba.grape.graph.loader.tableLoader.SQLRecord;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;

public class StringConverter implements ColumnConverterBase {
  private Text value;

  public StringConverter() {
    value = new Text();
  }

  @Override
  public void arrayRecordToSQLRecord(int index, ArrayRecord recIn, SQLRecord recOut) {
    String data = recIn.getString(index);
    if (data == null) {
      value.set("(null)");
    } else {
      value.set(data);
    }
    recOut.set(index, value);
  }

  @Override
  public void writableToArrayRecord(int index, Writable valIn, ArrayRecord recOut) {
    recOut.set(index, ((Text) valIn).toString());
  }
  @Override
  public void stringToArrayRecord(int index, String valIn, ArrayRecord recOut) {
    recOut.set(index, valIn);
  }
}
