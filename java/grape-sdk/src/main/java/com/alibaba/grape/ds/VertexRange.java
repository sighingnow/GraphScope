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

package com.alibaba.grape.ds;

import com.alibaba.fastffi.*;

import java.util.function.Consumer;

import static com.alibaba.grape.utils.CppClassName.GRAPE_VERTEX_RANGE;
import static com.alibaba.grape.utils.CppHeaderName.GRAPE_VERTEX_ARRAY_H;
import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(GRAPE_VERTEX_ARRAY_H)
@FFITypeAlias(GRAPE_VERTEX_RANGE)
@CXXTemplate(cxx = "uint32_t", java = "java.lang.Integer")
@CXXTemplate(cxx = "uint64_t", java = "java.lang.Long")
public interface VertexRange<VID> extends FFIPointer, CXXPointer, CXXValueRange<Vertex<VID>> {
    //public interface VertexRange<VID> extends FFIPointer, CXXPointer{
    @FFIFactory
    interface Factory<VID> {
        VertexRange<VID> create();
    }

    /**
     * Return the Begin vertex for this VertexRange.
     * Note that invoking this methods multiple times will return the same reference,
     * java object is not created when this method is called.
     *
     * @return Vertex<VID> return the begin vertex reference
     */
    @CXXReference Vertex<VID> begin();

    @CXXReference Vertex<VID> end();

    long size();

    void SetRange(@CXXValue VID begin, @CXXValue VID end);

    //  @Override
    default void forEach2(Consumer<Vertex<VID>> action) {
        Vertex<VID> vertex = begin();
        VID endValue = end().GetValue();
        //TODO: make this safe
        while (!vertex.GetValue().equals(endValue)) {
            action.accept(vertex);
            vertex.inc();
        }
    }

    //  default Iterable<Vertex<VID>> locals(){
//    return (Iterable<Vertex<VID>>) () -> new Iterator<Vertex<VID>>() {
//      private Vertex<VID> end = end();
//      private Vertex<VID> cur = begin();
//      private boolean flag = !cur.eq(end);
//      @Override
//      public boolean hasNext() {
//        if (flag == false){
//          cur.inc();
//          flag = !cur.eq(end);
//        }
//        return flag;
//      }
//
//      @Override
//      public Vertex<VID> next() {
//        flag = false;
//        return cur;
//      }
//    };
//  }
}
