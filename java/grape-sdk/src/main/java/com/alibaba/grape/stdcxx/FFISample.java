package com.alibaba.grape.stdcxx;

import com.alibaba.fastffi.*;

import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@FFIMirror
@FFINameSpace("sample")
@FFITypeAlias("FFIMirrorSample")
public interface FFISample extends CXXPointer, FFIJava {
    //Avoid incurring hashcode gen for vector<vector<>>
    default int javaHashCode() {
        return intField() + intProperty();
    }

    static FFISample create() {
        return factory.create();
    }

    static FFISample createStack() {
        return factory.createStack();
    }

    Factory factory = FFITypeFactory.getFactory(FFISample.class);

    @FFIFactory
    interface Factory {
        FFISample create();

        @CXXValue FFISample createStack();
    }

    @FFIGetter
    int intField();

    @FFISetter
    void intField(int value);

    @FFIProperty
    int intProperty();

    @FFIProperty
    void intProperty(int value);

    @FFIGetter
    @CXXReference
    FFIByteString stringField();

    @FFIGetter
    @CXXReference
    FFIVector<Integer> intVectorField();

    @FFIGetter
    @CXXReference
    FFIVector<Long> longVectorField();

    @FFIGetter
    @CXXReference
    FFIVector<Double> doubleVectorField();

    @FFIGetter
    @CXXReference
    FFIVector<FFIVector<Integer>> intVectorVectorField();

    @FFIGetter
    @CXXReference
    FFIVector<FFIVector<Double>> doubleVectorVectorField();

    @FFIGetter
    @CXXReference
    FFIVector<FFIVector<Long>> longVectorVectorField();
}