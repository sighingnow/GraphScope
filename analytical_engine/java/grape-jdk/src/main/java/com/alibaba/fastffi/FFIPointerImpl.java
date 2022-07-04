package com.alibaba.fastffi;

public class FFIPointerImpl implements FFIPointer{
    public long address;

    public FFIPointerImpl() {this.address = 0;}

    public FFIPointerImpl(long address) {
        this.address = address;
    }

    public final long getAddress() {
        return this.address;
    }
}
