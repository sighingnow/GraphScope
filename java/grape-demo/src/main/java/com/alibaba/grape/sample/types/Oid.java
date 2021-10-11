package com.alibaba.grape.sample.types;

import com.alibaba.ffi.*;

import static com.alibaba.grape.utils.JNILibraryName.JAVA_APP_JNI_LIBRARY;


//@FFIGen(library = "pie-user")
//@FFIMirror
//@FFINameSpace("sample")
//@FFITypeAlias("Oid")
//public interface Oid extends CXXPointer,FFIJava {
//    static Oid create() {
//        return factory.create();
//    }
//    Oid.Factory factory = FFITypeFactory.getFactory(Oid.class);
//    @FFIFactory
//    interface Factory {
//        Oid create();
//    }
//
//    @FFIGetter long id();
//    @FFISetter void id(long value);
//
//    default int javaHashCode(){
//        return (int)id();
//    }
//
////    @FFIGetter FFIByteString data();
////    @FFISetter void data(FFIByteString str);
//}

@FFIGen(library = JAVA_APP_JNI_LIBRARY)
@FFIMirror
@FFINameSpace("sample")
@FFITypeAlias("Oid")
public interface Oid extends CXXPointer, FFIJava {
    @FFIGetter
    long id();

    @FFISetter
    void id(long value);

    static Oid create() {
        return factory.create();
    }

    Oid.Factory factory = FFITypeFactory.getFactory(Oid.class);

    @FFIFactory
    interface Factory {
        Oid create();
    }

    default int javaHashCode() {
        return (int) id();
    }
}


