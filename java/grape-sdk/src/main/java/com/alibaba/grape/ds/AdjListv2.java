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

package com.alibaba.grape.ds;

import java.util.Iterator;
import java.util.function.Consumer;

public class AdjListv2<VID_T, EDATA_T> implements Iterable<Nbr<VID_T, EDATA_T>> {
    private Nbr<VID_T, EDATA_T> beginNbr;
    private Nbr<VID_T, EDATA_T> endNbr;
    private long endNbrAddr;
    private long elementSize;
    private long beginNbrAddr;

    public AdjListv2(Nbr<VID_T, EDATA_T> inbegin, Nbr<VID_T, EDATA_T> inEnd) {
        beginNbr = inbegin;
        endNbr = inEnd;
        elementSize = inbegin.elementSize();
        endNbrAddr = endNbr.getAddress();
        beginNbrAddr = beginNbr.getAddress();
    }

    public Nbr<VID_T, EDATA_T> begin() {
        return beginNbr;
    }

    public Nbr<VID_T, EDATA_T> end() {
        return endNbr;
    }

    public long size() {
        return (endNbrAddr - beginNbrAddr) / elementSize;
    }

    public Iterator<Nbr<VID_T, EDATA_T>> iterator() {
        return new Iterator<Nbr<VID_T, EDATA_T>>() {
            private Nbr<VID_T, EDATA_T> cur = begin().moveTo(beginNbrAddr);
            private long curAddr = beginNbrAddr;

            @Override
            public boolean hasNext() {
                return curAddr != endNbrAddr;
            }

            @Override
            public Nbr<VID_T, EDATA_T> next() {
                cur.setAddress(curAddr);
                curAddr += elementSize;
                return cur;
            }
        };
    }

    public void forEachVertex(Consumer<Nbr<VID_T, EDATA_T>> consumer) {
        Nbr<VID_T, EDATA_T> cur = begin().moveTo(beginNbrAddr);
        while (cur.getAddress() != endNbrAddr) {
            consumer.accept(cur);
            cur.addV(elementSize);
        }
    }
}
