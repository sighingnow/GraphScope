/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.grape.stdcxx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;

import java.io.IOException;
import java.io.OutputStream;

import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;

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
