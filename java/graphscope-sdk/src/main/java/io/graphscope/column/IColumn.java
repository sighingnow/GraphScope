package io.graphscope.column;

import com.alibaba.ffi.*;

import static com.alibaba.grape.utils.CPP_HEADER_STRINGS.CORE_JAVA_TYPE_ALIAS_H;
import static io.graphscope.utils.CPP_CLASS.I_COLUMN;
import static io.graphscope.utils.CPP_JNI_LIBRARY.VINEYARD_JNI_LIBRARY;


@FFIGen(library = VINEYARD_JNI_LIBRARY)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(I_COLUMN)
public interface IColumn extends FFIPointer {
    @CXXValue FFIByteString name();

    @FFINameAlias("set_name")
    void setName(@CXXReference FFIByteString name);
}
