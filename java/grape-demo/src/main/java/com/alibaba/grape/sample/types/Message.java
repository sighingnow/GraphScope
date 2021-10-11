package com.alibaba.grape.sample.types;

import com.alibaba.ffi.*;

import static com.alibaba.grape.utils.JNILibraryName.JAVA_APP_JNI_LIBRARY;

@FFIGen(library = JAVA_APP_JNI_LIBRARY)
@FFIMirror
@FFINameSpace("sample")
@FFITypeAlias("Message")
public interface Message extends CXXPointer {
    //    @FFIGetter double data();
//    @FFISetter void data(double value);
//
//    @FFIGetter @CXXReference FFIByteString label();
//    @FFISetter void label(@CXXReference FFIByteString str);
    @FFIGetter
    long data();

    @FFISetter
    void data(long value);

    static Message create() {
        return factory.create();
    }

    Message.Factory factory = FFITypeFactory.getFactory(Message.class);

    @FFIFactory
    interface Factory {
        Message create();
    }
}

