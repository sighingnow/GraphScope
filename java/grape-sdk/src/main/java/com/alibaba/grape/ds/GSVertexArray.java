package com.alibaba.grape.ds;

import com.alibaba.ffi.*;

import static com.alibaba.grape.utils.CPP_CLASSES_STRINGS.*;
import static com.alibaba.grape.utils.CPP_HEADER_STRINGS.*;
import static com.alibaba.grape.utils.CPP_LIBRARY_STRINGS.GRAPE_JNI_LIBRARY;


@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(GS_CORE_CONFIG_H)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@CXXHead(GRAPE_VERTEX_ARRAY_H)
@CXXHead(system = "cstdint")
@FFITypeAlias(GS_VERTEX_ARRAY)
@CXXTemplate(cxx = {"double"}, java = {"java.lang.Double"})
@CXXTemplate(cxx = {"uint64_t"}, java = {"java.lang.Long"})
@CXXTemplate(cxx = {"int64_t"}, java = {"java.lang.Long"})
@CXXTemplate(cxx = {"uint32_t"}, java = {"java.lang.Integer"})
@CXXTemplate(cxx = {"int32_t"}, java = {"java.lang.Integer"})
public interface GSVertexArray<T> extends FFIPointer, CXXPointer {
    @FFIFactory
    interface Factory<T> {
        GSVertexArray<T> create();
    }

    void Init(@CXXReference @FFITypeAlias(GRAPE_VERTEX_RANGE + "<uint64_t>") VertexRange<Long> range);

    void Init(@CXXReference @FFITypeAlias(GRAPE_VERTEX_RANGE + "<uint64_t>") VertexRange<Long> range, @CXXReference T val);

    void SetValue(@CXXReference T val);

    void SetValue(@CXXReference @FFITypeAlias(GRAPE_VERTEX_RANGE + "<uint64_t>") VertexRange<Long> range, @CXXReference T val);

    @FFINameAlias("SetValue")
    void set(@CXXReference @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") Vertex<Long> range, @CXXReference T val);

    @FFINameAlias("GetValue")
    @CXXOperator("[]")
    @CXXReference T get(@CXXReference @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") Vertex<Long> v);

    @CXXReference @FFITypeAlias(GRAPE_VERTEX_RANGE + "<uint64_t>") VertexRange<Long> GetVertexRange();
    // void Clear();
}

