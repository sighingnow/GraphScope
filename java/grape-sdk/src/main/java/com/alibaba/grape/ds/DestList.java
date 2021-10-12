package com.alibaba.grape.ds;

import com.alibaba.fastffi.*;

import static com.alibaba.grape.utils.CppClassName.GRAPE_DEST_LIST;
import static com.alibaba.grape.utils.CppHeaderName.GRAPE_ADJ_LIST_H;
import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(GRAPE_ADJ_LIST_H)
@FFITypeAlias(GRAPE_DEST_LIST)
public interface DestList extends FFIPointer, CXXPointer, CXXPointerRange<FidPointer> {
    @FFIGetter
    FidPointer begin();

    @FFIGetter
    FidPointer end();

    default boolean Empty() {
        return begin().equals(end());
    }

    default boolean NotEmpty() {
        return !Empty();
    }
}
