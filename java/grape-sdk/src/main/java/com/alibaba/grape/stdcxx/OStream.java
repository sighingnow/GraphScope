package com.alibaba.grape.stdcxx;

import com.alibaba.ffi.CXXHead;
import com.alibaba.ffi.FFIGen;
import com.alibaba.ffi.FFIPointer;
import com.alibaba.ffi.FFITypeAlias;

import java.io.IOException;
import java.io.OutputStream;

import static com.alibaba.grape.utils.CPP_LIBRARY_STRINGS.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(value = {}, system = {"ostream"})
@FFITypeAlias("std::ostream")
public interface OStream extends FFIPointer {
    /**
     * The signature of ostream is `ostream& put(const char c)`
     * we will generate a native method:
     * public void put(byte c) {
     * nativePut(getAddress(), c);
     * }
     * native void nativePut(long address, byte c);
     * In the generated JNI cxx file:
     * void Java_xxx_nativePut(JNIEnv*, jclass, jlong address, jbyte c) {
     * std::ostream* obj = reinterpret_cast<std::ostream*>(address);
     * obj->put(c);
     * }
     *
     * @param c
     */
    void put(byte c);
    // TODO: array is not supported now
    // void write(byte[] buf, int length);

    default OutputStream outputStream() {
        return new OutputStream() {
            @Override
            public void write(int i) throws IOException {
                OStream.this.put((byte) i);
            }

            public void write(byte[] buf, int begin, int length) throws IOException {
                //                if (begin == 0) {
                //                    OStream.this.write(buf, length);
                //                } else {
                //                    byte[] newBuf = Arrays.copyOfRange(buf, begin, begin + length);
                //                    OStream.this.write(newBuf, length);
                //                }
                int end = begin + length;
                for (int i = begin; i < end; i++) {
                    OStream.this.put(buf[i]);
                }
            }
        };
    }
}
