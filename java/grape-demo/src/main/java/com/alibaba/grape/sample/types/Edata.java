package com.alibaba.grape.sample.types;

import com.alibaba.ffi.*;

import static com.alibaba.grape.utils.CPP_LIBRARY_STRINGS.JAVA_APP_JNI_LIBRARY;

@FFIGen(library = JAVA_APP_JNI_LIBRARY)
@FFIMirror
@FFINameSpace("sample")
@FFITypeAlias("Edata")
public interface Edata extends CXXPointer {
    @FFIGetter
    double data();

    @FFISetter
    void data(double value);

    static Edata create() {
        return factory.create();
    }

    Edata.Factory factory = FFITypeFactory.getFactory(Edata.class);

    @FFIFactory
    interface Factory {
        Edata create();
    }

}
