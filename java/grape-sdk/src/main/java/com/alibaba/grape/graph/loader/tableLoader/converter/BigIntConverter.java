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
