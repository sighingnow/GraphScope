package com.alibaba.grape.stdcxx;

import com.alibaba.fastffi.*;
import sun.misc.Unsafe;

import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(system = "string")
@FFITypeAlias("std::string")
public interface StdString extends FFIPointer, CXXValueRange<StdString.Iterator> {
    Factory factory = FFITypeFactory.getFactory(Factory.class, StdString.class);

    @FFIFactory
    interface Factory {
        StdString create();

        default StdString create(@CXXValue String string) {
            StdString std = create();
            std.fromJavaString(string);
            return std;
        }

        StdString create(@CXXReference StdString string);
    }

    // @CXXOperator("*&") @CXXValue String toString();

    default void fromJavaString(String string) {
        byte[] bytes = string.getBytes();
        long size = bytes.length * Unsafe.ARRAY_BOOLEAN_INDEX_SCALE;
        resize(size);
        UnsafeHolder.U.copyMemory(bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, data(), size);
    }

    default String toJavaString() {
        byte[] data = new byte[(int) size()];
        UnsafeHolder.U.copyMemory(null, data(), data, Unsafe.ARRAY_BYTE_BASE_OFFSET,
                data.length * Unsafe.ARRAY_BOOLEAN_INDEX_SCALE);
        return new String(data);
    }

    @FFIGen(library = "grape-lite-jni")
    @CXXHead(system = "string")
    @FFITypeAlias("std::string::iterator")
    interface Iterator extends CXXValueRangeElement<Iterator>, FFIPointer {
        @CXXOperator("*")
        byte indirection();

        @CXXOperator("*&")
        @CXXValue Iterator copy();

        @CXXOperator("++")
        @CXXReference Iterator inc();

        @CXXOperator(value = "==")
        boolean eq(@CXXReference Iterator rhs);
    }

    long size();

    long data();

    void resize(long size);

    void clear();

    void push_back(byte c);

    /**
     * The actual String returns a reference but we can use a value.
     *
     * @return
     */
    byte at(long index);

    @CXXValue Iterator begin();

    @CXXValue Iterator end();

    CCharPointer c_str();

    @CXXReference StdString append(@CXXReference StdString rhs);

    long find(@CXXReference StdString str, long pos);

    default long find(@CXXReference StdString str) {
        return find(str, 0);
    }

    long find(byte c, long pos);

    default long find(byte c) {
        return find(c, 0);
    }

    @CXXValue StdString substr(long pos, long len);

    default @CXXValue StdString substr(long pos) {
        // std::string::npos
        return substr(pos, -1L);
    }

    long find_first_of(@CXXReference StdString str, long pos);

    default long find_first_of(@CXXReference StdString str) {
        return find_first_of(str, 0);
    }

    long find_last_of(@CXXReference StdString str, long pos);

    default long find_last_of(@CXXReference StdString str) {
        return find_last_of(str, -1L);
    }

    long find_first_of(byte c, long pos);

    default long find_first_of(byte c) {
        return find_first_of(c, 0);
    }

    long find_last_of(byte c, long pos);

    default long find_last_of(byte c) {
        return find_last_of(c, -1L);
    }

    int compare(@CXXReference StdString str);
}
