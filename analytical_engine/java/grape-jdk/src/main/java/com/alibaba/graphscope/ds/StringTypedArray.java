package com.alibaba.graphscope.ds;

import static com.alibaba.graphscope.utils.CppClassName.GS_STRING_MUTABLE_TYPE_ARRAY;
import static com.alibaba.graphscope.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.stdcxx.StdVector;

@FFIGen
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@CXXHead(system = "cstdint")
@FFITypeAlias(GS_STRING_MUTABLE_TYPE_ARRAY)
public interface StringTypedArray extends FFIPointer {

    @FFINameAlias("GetRawBytes")
    @CXXReference @FFITypeAlias("std::vector<char>") StdVector<Byte> getRawBytes();

    @FFINameAlias("GetLength") long getLength();
}
