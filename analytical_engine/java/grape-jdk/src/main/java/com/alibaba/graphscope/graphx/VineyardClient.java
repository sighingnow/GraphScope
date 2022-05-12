package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.FFIByteString;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen(library = "grape-jni")
@CXXHead(CppHeaderName.VINEYARD_CLIENT_H)
@FFITypeAlias(CppClassName.VINEYARD_CLIENT)
public interface VineyardClient extends FFIPointer {
    @FFINameAlias("Connect")
    void connect(FFIByteString endPoint);

    @FFIFactory
    interface Factory{
        VineyardClient create();
    }
}
