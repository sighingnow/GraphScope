package com.alibaba.grape.ds;

import com.alibaba.ffi.*;

import static com.alibaba.grape.utils.CPP_CLASSES_STRINGS.PROJECTED_NBR;
import static com.alibaba.grape.utils.CPP_HEADER_STRINGS.ARROW_PROJECTED_FRAGMENT_H;
import static com.alibaba.grape.utils.CPP_HEADER_STRINGS.CORE_JAVA_TYPE_ALIAS_H;
import static com.alibaba.grape.utils.CPP_LIBRARY_STRINGS.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(ARROW_PROJECTED_FRAGMENT_H)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(PROJECTED_NBR)
@CXXTemplate(cxx = {"uint64_t", "int64_t"}, java = {"java.lang.Long", "java.lang.Long"})
@CXXTemplate(cxx = {"uint64_t", "int32_t"}, java = {"java.lang.Long", "java.lang.Integer"})
@CXXTemplate(cxx = {"uint64_t", "double"}, java = {"java.lang.Long", "java.lang.Double"})
@CXXTemplate(cxx = {"uint64_t", "uint64_t"}, java = {"java.lang.Long", "java.lang.Long"})
@CXXTemplate(cxx = {"uint64_t", "uint32_t"}, java = {"java.lang.Long", "java.lang.Integer"})
public interface ProjectedNbr<VID_T, EDATA_T> extends FFIPointer {
    @CXXValue Vertex<VID_T> neighbor();

    @FFINameAlias("edge_id") long edgeId();

    EDATA_T data();

    @CXXOperator("++")
    @CXXReference ProjectedNbr<VID_T, EDATA_T> inc();

    @CXXOperator("==")
    boolean eq(@CXXReference ProjectedNbr<VID_T, EDATA_T> rhs);

    @CXXOperator("--")
    @CXXReference ProjectedNbr<VID_T, EDATA_T> dec();
}
