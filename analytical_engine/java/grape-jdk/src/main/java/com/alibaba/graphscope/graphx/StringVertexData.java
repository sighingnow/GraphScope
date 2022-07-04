package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIByteString;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISerializable;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.ImmutableTypedArray;
import com.alibaba.graphscope.ds.StringTypedArray;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;

@FFIGen(library = "grape-jni")
@CXXHead(CppHeaderName.CORE_JAVA_GRAPHX_VERTEX_DATA_H)
@CXXHead(CppHeaderName.CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(CppClassName.GS_STRING_VERTEX_DATA)
public interface StringVertexData<VID,T> extends FFISerializable {
    long id();
    /**
     * Could contain outer vertices data
     * @return nums
     */
    @FFINameAlias("VerticesNum") VID verticesNum();

    @FFINameAlias("GetVdataArray") @CXXReference @FFITypeAlias("gs::graphx::ImmutableTypedArray<std::string>") StringTypedArray getVdataArray();

}
