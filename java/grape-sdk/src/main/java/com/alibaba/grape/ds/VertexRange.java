package com.alibaba.grape.ds;

import com.alibaba.ffi.*;

import java.util.function.Consumer;

import static com.alibaba.grape.utils.CPP_CLASSES_STRINGS.GRAPE_VERTEX_RANGE;
import static com.alibaba.grape.utils.CPP_HEADER_STRINGS.GRAPE_VERTEX_ARRAY_H;
import static com.alibaba.grape.utils.CPP_LIBRARY_STRINGS.GRAPE_JNI_LIBRARY;

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
