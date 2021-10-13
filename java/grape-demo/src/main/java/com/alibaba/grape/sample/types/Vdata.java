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
