package com.alibaba.grape.stdcxx;

import com.alibaba.ffi.*;

import static com.alibaba.grape.utils.CPP_LIBRARY_STRINGS.GRAPE_JNI_LIBRARY;


@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(value = {"stdint.h"}, system = {"unordered_map"})
@FFITypeAlias("std::unordered_map")
@CXXTemplate(cxx = {"unsigned", "uint64_t"}, java = {"java.lang.Integer", "java.lang.Long"})
public interface StdUnorderedMap<KEY_T, VALUE_T> extends CXXPointer {
    @FFIFactory
    interface Factory<KEY_T, VALUE_T> {
        StdUnorderedMap<KEY_T, VALUE_T> create();
    }

    int size();

    boolean empty();

    @CXXReference
    @CXXOperator("[]")
    VALUE_T get(@CXXReference KEY_T key);

    @CXXOperator("[]")
    void set(@CXXReference KEY_T key, @CXXReference VALUE_T value);
}
