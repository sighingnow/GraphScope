package com.alibaba.grape.ds;

import com.alibaba.fastffi.*;

import static com.alibaba.grape.utils.CppClassName.GRAPE_ADJ_LIST;
import static com.alibaba.grape.utils.CppHeaderName.GRAPE_ADJ_LIST_H;
import static com.alibaba.grape.utils.CppHeaderName.GRAPE_TYPES_H;
import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead({GRAPE_ADJ_LIST_H, GRAPE_TYPES_H})
@FFITypeAlias(GRAPE_ADJ_LIST)
@CXXTemplate(cxx = {"uint64_t", "double"},
        java = {"Long", "Double"})
@CXXTemplate(cxx = {"uint64_t", "jdouble"},
        java = {"Long", "Double"})
public interface AdjList<VID, EDATA>
        extends FFIPointer, CXXPointer, CXXPointerRange<Nbr<VID, EDATA>> {
    default Nbr<VID, EDATA> begin() {
        return begin_pointer();
    }

    default Nbr<VID, EDATA> end() {
        return end_pointer();
    }

    Nbr<VID, EDATA> begin_pointer();

    Nbr<VID, EDATA> end_pointer();

    @FFINameAlias("Size")
    long size();
}
