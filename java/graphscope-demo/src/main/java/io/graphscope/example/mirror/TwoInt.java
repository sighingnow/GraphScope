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
