package com.alibaba.grape.sample.types;

import com.alibaba.fastffi.*;

import static com.alibaba.grape.utils.JNILibraryName.JAVA_APP_JNI_LIBRARY;

@FFIGen(library = JAVA_APP_JNI_LIBRARY)
@FFIMirror
@FFINameSpace("sample")
@FFITypeAlias("Vdata")
public interface Vdata extends CXXPointer {
    //    @FFIGetter int intValue();
//    @FFISetter void intValue(int value);
    @FFIGetter
    long data();

    @FFISetter
    void data(long value);

    static Vdata create() {
        return factory.create();
    }

    Vdata.Factory factory = FFITypeFactory.getFactory(Vdata.class);

    @FFIFactory
    interface Factory {
        Vdata create();
    }
}
