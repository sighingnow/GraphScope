package io.graphscope.column;


import com.alibaba.ffi.*;
import com.alibaba.grape.ds.GSVertexArray;
import com.alibaba.grape.ds.Vertex;

import static com.alibaba.grape.utils.CppClassName.*;
import static com.alibaba.grape.utils.CppHeaderName.ARROW_PROJECTED_FRAGMENT_H;
import static com.alibaba.grape.utils.CppHeaderName.GRAPE_TYPES_H;
import static io.graphscope.utils.CppClassName.ARROW_FRAGMENT;
import static io.graphscope.utils.CppClassName.DOUBLE_COLUMN;
import static io.graphscope.utils.CppHeaderName.ARROW_FRAGMENT_H;
import static io.graphscope.utils.CppHeaderName.CORE_JAVA_TYPE_ALIAS_H;
import static io.graphscope.utils.JNILibraryName.VINEYARD_JNI_LIBRARY;

@FFIGen(library = VINEYARD_JNI_LIBRARY)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@CXXHead(system = "cstdint")
@CXXHead(GRAPE_TYPES_H)
@CXXHead(ARROW_FRAGMENT_H)
@CXXHead(ARROW_PROJECTED_FRAGMENT_H)
@FFITypeAlias(DOUBLE_COLUMN)
@CXXTemplate(cxx = {ARROW_FRAGMENT + "<int64_t>"}, java = {"io.graphscope.fragment.ArrowFragment<java.lang.Long>"})
@CXXTemplate(cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,grape::EmptyType,int64_t>"},
        java = {"com.alibaba.grape.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,com.alibaba.grape.ds.EmptyType,java.lang.Long>"})
public interface DoubleColumn<FRAG_T> extends IColumn {

    double at(@CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex);

    void set(@CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex, double value);

    @CXXReference @FFITypeAlias(GS_VERTEX_ARRAY + "<double>") GSVertexArray<Double> data();

}
