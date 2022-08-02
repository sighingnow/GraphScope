package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISerializable;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.arrow.array.ArrowArrayBuilder;
import com.alibaba.graphscope.stdcxx.StdSharedPtr;
import com.alibaba.graphscope.stdcxx.StdVector;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen(library = "grape-jni")
@CXXHead(CppHeaderName.CORE_JAVA_GRAPHX_VERTEX_DATA_H)
@FFITypeAlias(CppClassName.GS_STRING_VERTEX_DATA_BUILDER)
public interface StringVertexDataBuilder<VID,T> extends FFISerializable {

    @FFINameAlias("Init")
    void init(long frag_vnums, @CXXReference @FFITypeAlias("std::vector<char>") StdVector<Byte> vector,
        @CXXReference @FFITypeAlias("std::vector<int32_t>") StdVector<Integer> length);

//    @FFINameAlias("SetBitsetWords")
//    void setBitsetWords(@CXXReference @FFITypeAlias("arrow::Int64Builder") ArrowArrayBuilder<Long> words);

    @FFINameAlias("MySeal")
    @CXXValue StdSharedPtr<StringVertexData<VID,T>> seal(@CXXReference VineyardClient client);

    @FFIFactory
    interface Factory<VID,T> {

        StringVertexDataBuilder<VID,T> create();
    }
}
