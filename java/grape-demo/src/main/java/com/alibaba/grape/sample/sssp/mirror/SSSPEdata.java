package com.alibaba.grape.sample.sssp.mirror;

import com.alibaba.fastffi.*;

import static com.alibaba.grape.utils.JNILibraryName.JAVA_APP_JNI_LIBRARY;

@FFIGen(library = JAVA_APP_JNI_LIBRARY)
@FFIMirror
@FFINameSpace("sample")
@FFITypeAlias("SSSPEdata")
public interface SSSPEdata extends CXXPointer {
    @FFIGetter
    double value();

    @FFISetter
    void value(double val);

    static SSSPEdata create() {
        return factory.create();
    }

    SSSPEdata.Factory factory = FFITypeFactory.getFactory(SSSPEdata.class);

    @FFIFactory
    interface Factory {
        SSSPEdata create();
    }
}
