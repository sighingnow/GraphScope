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

package com.alibaba.grape.utils;

public class Number128 {
  private long lowValue;
  private long hiValue;

  public Number128(long lowValue, long hiValue) {
    this.setLowValue(lowValue);
    this.setHiValue(hiValue);
  }

  public long getLowValue() {
    return lowValue;
  }

  public long getHiValue() {
    return hiValue;
  }

  public void setLowValue(long lowValue) {
    this.lowValue = lowValue;
  }

  public void setHiValue(long hiValue) {
    this.hiValue = hiValue;
  }
}
