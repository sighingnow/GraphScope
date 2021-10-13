/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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