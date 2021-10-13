/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
