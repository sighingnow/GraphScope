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
@FFITypeAlias("Message")
public interface Message extends CXXPointer {
    // @FFIGetter double data();
    // @FFISetter void data(double value);
    //
    // @FFIGetter @CXXReference FFIByteString label();
    // @FFISetter void label(@CXXReference FFIByteString str);
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
