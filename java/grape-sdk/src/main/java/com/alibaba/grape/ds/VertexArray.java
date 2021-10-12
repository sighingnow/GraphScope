package com.alibaba.grape.ds;

import com.alibaba.fastffi.*;

import static com.alibaba.grape.utils.CppClassName.GRAPE_VERTEX_ARRAY;
import static com.alibaba.grape.utils.CppHeaderName.GRAPE_VERTEX_ARRAY_H;
import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(GRAPE_VERTEX_ARRAY_H)
@FFITypeAlias(GRAPE_VERTEX_ARRAY)
@CXXTemplate(cxx = {"jboolean", "uint32_t"}, java = {"java.lang.Boolean", "java.lang.Integer"})
@CXXTemplate(cxx = {"jdouble", "uint32_t"}, java = {"java.lang.Double", "java.lang.Integer"})
@CXXTemplate(cxx = {"jdouble", "uint64_t"}, java = {"java.lang.Double", "java.lang.Long"})
@CXXTemplate(cxx = {"double", "uint64_t"}, java = {"java.lang.Double", "java.lang.Long"})
@CXXTemplate(cxx = {"jlong", "uint64_t"}, java = {"java.lang.Long", "java.lang.Long"})
public interface VertexArray<T, VID> extends FFIPointer, CXXPointer {
    @FFIFactory
    interface Factory<T, VID> {
        VertexArray<T, VID> create();
    }

    void Init(@CXXReference VertexRange<VID> range);

    void Init(@CXXReference VertexRange<VID> range, @CXXReference T val);

    void SetValue(@CXXReference T val);

    void SetValue(@CXXReference VertexRange<VID> range, @CXXReference T val);

    @FFINameAlias("SetValue")
    void set(@CXXReference Vertex<VID> range, @CXXReference T val);

    @FFINameAlias("GetValue")
    @CXXOperator("[]")
    @CXXReference T get(@CXXReference Vertex<VID> v);

    @CXXReference VertexRange<VID> GetVertexRange();
    // void Clear();
}
