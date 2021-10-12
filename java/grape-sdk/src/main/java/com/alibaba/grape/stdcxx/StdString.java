package com.alibaba.grape.stdcxx;

import com.alibaba.fastffi.*;
import sun.misc.Unsafe;

import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(system = "string")
@FFITypeAlias("std::string")
public interface StdString extends CXXPointer, CXXValueRange<StdString.Iterator>,
        FFIStringReceiver, FFIStringProvider {

    Factory factory = FFITypeFactory.getFactory(StdString.class);

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

    @FFIGen(library = "ffitest")
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
    long c_str();

    void resize(long size);

    void clear();

    void push_back(byte c);

    /**
     * The actual String returns a reference but we can use a value.
     * @return
     */
    byte at(long index);

    @CXXValue Iterator begin();
    @CXXValue Iterator end();

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

    int compare (@CXXReference StdString str);
}
