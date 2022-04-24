package com.alibaba.graphscope.ds;

import com.alibaba.fastffi.llvm4jni.runtime.JavaRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryMappedBufferWriter {

    private static Logger logger = LoggerFactory.getLogger(
        MemoryMappedBufferWriter.class.getName());

    static {
        try {
            System.loadLibrary("grape-jni");
            logger.info("load grape-jni success");
        } catch (Exception e) {
            throw new IllegalStateException("Fail to load library: grape-jni");
        }
    }

    private long startAddress, size;
    private long limit;
    private long currentAddress; //current address

    public MemoryMappedBufferWriter(long address, long size) {
        this.startAddress = address;
        this.size = size;
        this.limit = startAddress + size;
        this.currentAddress = startAddress;
    }

    public long position() {
        return currentAddress - startAddress;
    }

    public long remaining() {
        return limit - currentAddress;
    }

    public long limit() {
        return limit - startAddress;
    }

    public void position(long newPos) {
        if (newPos >= size || newPos < 0) {
            throw new IndexOutOfBoundsException("new pos out of bound: " + newPos);
        }
        currentAddress = startAddress + newPos;
    }

    public void writeInt(int value) {
        check(currentAddress + 4);
        JavaRuntime.putInt(currentAddress, value);
        currentAddress += 4;
    }

    public void writeLong(long value) {
        check(currentAddress + 8);
        JavaRuntime.putLong(currentAddress, value);
        currentAddress += 8;
    }

    public void writeLong(int index, long value) {
        check(startAddress + index);
        JavaRuntime.putLong(startAddress + index, value);
        //no increment address
    }


    public void writeDouble(double value) {
        check(currentAddress + 8);
        JavaRuntime.putDouble(currentAddress, value);
        currentAddress += 8;
    }

    private void check(long addr) {
        if (addr >= limit) {
            throw new IndexOutOfBoundsException(
                " out of bound, limit : " + limit + ", cur " + addr);
        }
    }

    @Override
    public String toString() {
        return "MemoryMappedBufferWriter{" +
            "startAddress=" + startAddress +
            ", size=" + size +
            ", limit=" + limit +
            ", currentAddress=" + currentAddress +
            '}';
    }
}
