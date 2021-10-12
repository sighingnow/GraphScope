package io.graphscope.column;

import com.alibaba.fastffi.*;

import static com.alibaba.grape.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;
import static io.graphscope.utils.CppClassName.I_COLUMN;
import static io.graphscope.utils.JNILibraryName.VINEYARD_JNI_LIBRARY;


@FFIGen(library = VINEYARD_JNI_LIBRARY)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(I_COLUMN)
public interface IColumn extends FFIPointer {
    @CXXValue FFIByteString name();

    @FFINameAlias("set_name")
    void setName(@CXXReference FFIByteString name);
}
