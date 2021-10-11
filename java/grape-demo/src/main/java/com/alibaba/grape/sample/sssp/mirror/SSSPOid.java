package com.alibaba.grape.sample.sssp.mirror;

import com.alibaba.ffi.*;

import static com.alibaba.grape.utils.JNILibraryName.JAVA_APP_JNI_LIBRARY;

@FFIGen(library = JAVA_APP_JNI_LIBRARY)
@FFIMirror
@FFINameSpace("sample")
@FFITypeAlias("SSSPOid")
public interface SSSPOid extends CXXPointer, FFIJava {
    @FFIGetter
    long value();

    @FFISetter
    void value(long val);

    static SSSPOid create() {
        return factory.create();
    }

    SSSPOid.Factory factory = FFITypeFactory.getFactory(SSSPOid.class);

    @FFIFactory
    interface Factory {
        SSSPOid create();
    }

    default int javaHashCode() {
        return (int) value();
    }
}