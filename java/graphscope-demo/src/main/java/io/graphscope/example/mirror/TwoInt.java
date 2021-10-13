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

package io.graphscope.example.mirror;

import com.alibaba.fastffi.*;

//How to hide this from user.
//@FFIGen(library = "pie-user")
@FFIMirror
@FFINameSpace("sample")
@FFITypeAlias("TwoInt")
public interface TwoInt extends FFIPointer, FFIJava {
    @FFIGetter
    int intField1();

    @FFIGetter
    int intField2();
}
