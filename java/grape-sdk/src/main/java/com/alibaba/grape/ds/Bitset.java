package com.alibaba.grape.ds;

import com.alibaba.fastffi.*;

import static com.alibaba.grape.utils.CppClassName.GRAPE_BIT_SET;
import static com.alibaba.grape.utils.CppHeaderName.GRAPE_BIT_SET_H;
import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;
import static com.alibaba.grape.utils.CppHeaderName.GRAPE_WORKER_COMM_SPEC_H;;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(value = {GRAPE_WORKER_COMM_SPEC_H, GRAPE_BIT_SET_H})
@FFITypeAlias(GRAPE_BIT_SET)
public interface Bitset extends FFIPointer, CXXPointer {

    Factory factory = FFITypeFactory.getFactory(Factory.class, Bitset.class);

    @FFIFactory
    interface Factory {
        Bitset create();
    }

    void init(long size);

    void clear();

//    void parallel_clear(int thread_num);

    boolean empty();

    boolean partial_empty(long begin, long end);

    boolean get_bit(long i);

    void set_bit(long i);

    boolean set_bit_with_ret(long i);

    void reset_bit(long i);

    boolean reset_bit_with_ret(long i);

    void swap(@CXXReference Bitset other);

    long count();

//    long parallel_count(int thread_num);

    long partial_count(long begin, long end);

//    long parallel_partial_count(int thread_num, long begin, long end);

    long get_word(long i);
    // long get_word_ptr(long i);
}
