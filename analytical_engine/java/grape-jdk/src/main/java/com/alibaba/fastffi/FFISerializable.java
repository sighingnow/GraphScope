package com.alibaba.fastffi;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public interface FFISerializable extends FFIPointer,FFISettablePointer {
    default void readObject(ObjectInputStream aInputStream) throws IOException {
        setAddress(aInputStream.readLong());
    }

    default void writeObject(ObjectOutputStream aOutputStream) throws IOException {
        aOutputStream.writeLong(getAddress());
    }
}
