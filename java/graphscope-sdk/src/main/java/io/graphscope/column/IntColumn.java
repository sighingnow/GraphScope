package io.graphscope.column;

import com.alibaba.fastffi.*;
import com.alibaba.grape.ds.GSVertexArray;
import com.alibaba.grape.ds.Vertex;

import static com.alibaba.grape.utils.CppClassName.*;
import static com.alibaba.grape.utils.CppHeaderName.ARROW_PROJECTED_FRAGMENT_H;
import static com.alibaba.grape.utils.CppHeaderName.GRAPE_TYPES_H;
import static io.graphscope.utils.CppClassName.ARROW_FRAGMENT;
import static io.graphscope.utils.CppClassName.INT_COLUMN;
import static io.graphscope.utils.CppHeaderName.ARROW_FRAGMENT_H;
import static io.graphscope.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;
import static io.graphscope.utils.JNILibraryName.VINEYARD_JNI_LIBRARY;

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
