package com.alibaba.graphscope.utils;

import com.alibaba.fastffi.llvm4jni.runtime.JavaRuntime;
import java.nio.MappedByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MappedBuffer {
    private static Logger logger = LoggerFactory.getLogger(MappedBuffer.class.getName());
    static{
        try {
            System.loadLibrary("grape-jni");
            logger.info("load grape-jni success");
        }
        catch (Exception e){
            throw new IllegalStateException("Fail to load library: grape-jni");
        }
    }

    private long startAddress, size;
    private long limit;
    private long currentAddress; //current address
    private String path;


    private MappedBuffer(String path,long address, long size){
        this.path = path;
        this.startAddress = address;
        this.size = size;
        this.limit = startAddress + size;
        this.currentAddress = startAddress;
    }

    public long position(){
        return currentAddress - startAddress;
    }

    public long remaining(){
        return limit - currentAddress;
    }

    public long limit(){
        return limit;
    }

    public void position(long newPos){
        if (newPos >= size || newPos < 0) {
            throw new IndexOutOfBoundsException("new pos out of bound: " + newPos);
        }
        currentAddress = startAddress + newPos;
    }

    public void writeInt(int value){
        check(currentAddress + 4);
        JavaRuntime.putInt(currentAddress, value);
        currentAddress += 4;
    }

    public void writeLong(long value){
        check(currentAddress + 8);
        JavaRuntime.putLong(currentAddress, value);
        currentAddress += 8;
    }

    public void writeLong(int index, long value){
        check(startAddress + index);
        JavaRuntime.putLong(startAddress + index, value);
        //no increment address
    }


    public void writeDouble(double value){
        check(currentAddress + 8);
        JavaRuntime.putDouble(currentAddress, value);
        currentAddress += 8;
    }

    public int readInt(long offset){
        checkOffset(offset + 4);
        return JavaRuntime.getInt(startAddress + offset);
    }
    public long readLong(long offset){
        checkOffset(offset + 8);
        return JavaRuntime.getLong(startAddress + offset);
    }
    public double readDouble(long offset){
        checkOffset(offset + 8);
        return JavaRuntime.getDouble(startAddress + offset);
    }

    public void checkOffset(long offset){
        if (offset > size){
            throw new IndexOutOfBoundsException(" out of bound, size : " + size + ", cur " + offset);
        }
    }

    /**
     * check whether last bit is in range
     * @param addr start + size
     */
    private void check(long addr){
        if (addr > limit){
            throw new IndexOutOfBoundsException(" out of bound, limit : " + limit + ", cur " + addr);
        }
    }

    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("MappedBuffer(path=" );
        sb.append(path);
        sb.append(",address=");
        sb.append(startAddress);
        sb.append(",size=");
        sb.append(size);
        sb.append(")");
        return sb.toString();
    }

    public static native long create(String path, long size);

    public static MappedBuffer mapToFile(String path, long size){
        if (size <= 0){
            logger.error("Size illegal " + size);
            return null;
        }
        long address = create(path, size);
	if (address <= 0){
	    throw new IllegalStateException("map failed: " + path + " size: " + size);
        }
        return new MappedBuffer(path, address, size);
    }
}
