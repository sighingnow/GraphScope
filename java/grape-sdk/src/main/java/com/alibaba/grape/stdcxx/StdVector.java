package com.alibaba.grape.stdcxx;

import com.alibaba.ffi.*;

import static com.alibaba.grape.utils.CPP_CLASSES_STRINGS.GS_VERTEX_ARRAY;
import static com.alibaba.grape.utils.CPP_HEADER_STRINGS.CORE_JAVA_TYPE_ALIAS_H;
import static com.alibaba.grape.utils.CPP_LIBRARY_STRINGS.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(system = {"vector", "string"})
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias("std::vector")
@CXXTemplate(cxx = "jshort", java = "java.lang.Short")
@CXXTemplate(cxx = "jint", java = "java.lang.Integer")
@CXXTemplate(cxx = "jchar", java = "java.lang.Character")
@CXXTemplate(cxx = "jbyte", java = "java.lang.Byte")
@CXXTemplate(cxx = "jlong", java = "java.lang.Long")
@CXXTemplate(cxx = "jdouble", java = "java.lang.Double")
@CXXTemplate(cxx = "jfloat", java = "java.lang.Float")
@CXXTemplate(cxx = "jboolean", java = "java.lang.Boolean")
@CXXTemplate(cxx = "int64_t", java = "java.lang.Long")
@CXXTemplate(cxx = "char", java = "java.lang.Byte")
@CXXTemplate(cxx = "std::string", java = "com.alibaba.ffi.FFIByteString")
@CXXTemplate(cxx = "std::vector<char>", java = "com.alibaba.grape.stdcxx.StdVector<java.lang.Byte>")
@CXXTemplate(cxx = "std::vector<int64_t>",
        java = "com.alibaba.grape.stdcxx.StdVector<java.lang.Long>")
@CXXTemplate(cxx = "std::vector<jint>",
        java = "com.alibaba.grape.stdcxx.StdVector<java.lang.Integer>")
@CXXTemplate(cxx = "std::vector<std::string>",
        java = "com.alibaba.grape.stdcxx.StdVector<com.alibaba.ffi.FFIByteString>")
@CXXTemplate(cxx = GS_VERTEX_ARRAY + "<double>",
        java = "com.alibaba.grape.ds.GSVertexArray<java.lang.Double>")
public interface StdVector<E> extends CXXPointer {
    @FFIFactory
    interface Factory<E> {
        StdVector<E> create();
    }

    long size();

    @CXXOperator("[]")
    @CXXReference E get(long index);

    @CXXOperator("[]")
    void set(long index, @CXXReference E value);

    void push_back(@CXXValue E e);

    default void add(@CXXReference E value) {
        long size = size();
        long cap = capacity();
        if (size == cap) {
            reserve(cap << 1);
        }
        push_back(value);
    }

    default @CXXReference E append() {
        long size = size();
        long cap = capacity();
        if (size == cap) {
            reserve(cap << 1);
        }
        resize(size + 1);
        return get(size);
    }

    void clear();

    long data();

    long capacity();

    void reserve(long size);

    void resize(long size);
}