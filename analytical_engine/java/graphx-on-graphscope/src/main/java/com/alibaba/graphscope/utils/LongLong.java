package com.alibaba.graphscope.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class LongLong implements Writable {
    public long first;
    public long second;

    @Override
    public String toString() {
        return "LongLong{" +
            "first=" + first +
            ", second=" + second +
            '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(first);
        dataOutput.writeLong(second);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first = dataInput.readLong();
        second = dataInput.readLong();
    }
}
