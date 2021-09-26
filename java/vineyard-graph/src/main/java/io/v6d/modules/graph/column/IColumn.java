package io.v6d.modules.graph.column;

import com.alibaba.ffi.*;

import static io.v6d.modules.graph.utils.CPP_CLASS.I_COLUMN;
import static io.v6d.modules.graph.utils.CPP_HEADER.CORE_JAVA_TYPE_ALIAS_H;
import static io.v6d.modules.graph.utils.CPP_JNI_LIBRARY.VINEYARD_JNI_LIBRARY;

@FFIGen(library = VINEYARD_JNI_LIBRARY)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(I_COLUMN)
public interface IColumn extends FFIPointer {
    @CXXValue FFIByteString name();

    @FFINameAlias("set_name")
    void setName(@CXXReference FFIByteString name);
}
