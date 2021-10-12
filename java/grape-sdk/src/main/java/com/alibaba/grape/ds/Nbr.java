package com.alibaba.grape.ds;

import com.alibaba.fastffi.*;

import static com.alibaba.grape.utils.CppClassName.GRAPE_NBR;
import static com.alibaba.grape.utils.CppHeaderName.GRAPE_ADJ_LIST_H;
import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(GRAPE_ADJ_LIST_H)
@FFITypeAlias(GRAPE_NBR)
@CXXTemplate(cxx = {"uint64_t", "std::string"},
        java = {"java.lang.Long", "com.alibaba.grape.stdcxx.StdString"})
@CXXTemplate(cxx = {"uint64_t", "jdouble"},
        java = {"Long", "Double"})
@CXXTemplate(cxx = {"uint64_t", "int64_t"},
        java = {"Long", "Long"})
@CXXTemplate(cxx = {"uint64_t", "double"},
        java = {"Long", "Double"})
public interface Nbr<VID, EDATA> extends FFIPointer, CXXPointerRangeElement<Nbr<VID, EDATA>> {
    @CXXOperator("*&")
    @CXXValue Nbr<VID, EDATA> copy();

    @FFIGetter
    @CXXReference Vertex<VID> neighbor();

    @FFIGetter
    @CXXReference EDATA data();

    @FFIFactory
    interface Factory<VID, EDATA> {
        Nbr<VID, EDATA> create();

        Nbr<VID, EDATA> create(VID lid);

        Nbr<VID, EDATA> craete(VID lid, @CXXReference EDATA edata);
    }
}
