package com.alibaba.grape.sample.sssp.mirror;

import com.alibaba.ffi.*;

import static com.alibaba.grape.utils.JNILibraryName.JAVA_APP_JNI_LIBRARY;

@FFIGen(library = JAVA_APP_JNI_LIBRARY)
@FFIMirror
@FFINameSpace("sample")
@FFITypeAlias("SSSPVdata")
public interface SSSPVdata extends CXXPointer {
    @FFIGetter
    long value();

    @FFISetter
    void value(long val);

    static SSSPVdata create() {
        return factory.create();
    }

    SSSPVdata.Factory factory = FFITypeFactory.getFactory(SSSPVdata.class);

    @FFIFactory
    interface Factory {
        SSSPVdata create();
    }
}
