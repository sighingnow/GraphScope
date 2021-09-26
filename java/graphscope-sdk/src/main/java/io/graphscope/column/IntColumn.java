package io.graphscope.column;

import com.alibaba.ffi.*;
import com.alibaba.grape.ds.GSVertexArray;
import com.alibaba.grape.ds.Vertex;

import static com.alibaba.grape.utils.CPP_CLASSES_STRINGS.*;
import static com.alibaba.grape.utils.CPP_HEADER_STRINGS.ARROW_PROJECTED_FRAGMENT_H;
import static com.alibaba.grape.utils.CPP_HEADER_STRINGS.GRAPE_TYPES_H;
import static io.graphscope.utils.CPP_CLASS.ARROW_FRAGMENT;
import static io.graphscope.utils.CPP_CLASS.INT_COLUMN;
import static io.graphscope.utils.CPP_HEADER.ARROW_FRAGMENT_H;
import static io.graphscope.utils.CPP_HEADER.CORE_JAVA_TYPE_ALIAS_H;
import static io.graphscope.utils.CPP_JNI_LIBRARY.VINEYARD_JNI_LIBRARY;

@FFIGen(library = VINEYARD_JNI_LIBRARY)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@CXXHead(system = "cstdint")
@CXXHead(ARROW_FRAGMENT_H)
@CXXHead(ARROW_PROJECTED_FRAGMENT_H)
@CXXHead(GRAPE_TYPES_H)
@FFITypeAlias(INT_COLUMN)
@CXXTemplate(cxx = {ARROW_FRAGMENT + "<int64_t>"}, java = {"io.graphscope.fragment.ArrowFragment<java.lang.Long>"})
@CXXTemplate(cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,grape::EmptyType,int64_t>"},
        java = {"com.alibaba.grape.fragment.ArrowProjectedFragment<Long,Long,com.alibaba.grape.ds.EmptyType,Long>"})
public interface IntColumn<FRAG_T> extends FFIPointer {
    double at(@CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex);

    void set(@CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex, int value);

    @CXXReference @FFITypeAlias(GS_VERTEX_ARRAY + "<uint32_t>") GSVertexArray<Integer> data();
}
