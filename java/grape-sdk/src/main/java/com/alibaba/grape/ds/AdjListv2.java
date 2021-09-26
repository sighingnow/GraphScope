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
