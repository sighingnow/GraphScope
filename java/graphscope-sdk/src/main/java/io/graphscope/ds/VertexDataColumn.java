package io.v6d.modules.graph.ds;

import com.alibaba.ffi.*;
import com.alibaba.grape.ds.Vertex;

import static com.alibaba.grape.utils.CPP_CLASSES_STRINGS.GRAPE_VERTEX;
import static io.v6d.modules.graph.utils.CPP_CLASS.VERTEX_DATA_COLUMN;
import static io.v6d.modules.graph.utils.CPP_HEADER.PROPERTY_GRAPH_UTILS_H;
import static io.v6d.modules.graph.utils.CPP_JNI_LIBRARY.VINEYARD_JNI_LIBRARY;

@FFIGen(library = VINEYARD_JNI_LIBRARY)
@CXXHead(PROPERTY_GRAPH_UTILS_H)
@FFITypeAlias(VERTEX_DATA_COLUMN)
@CXXTemplate(cxx = {"uint64_t"},
        java = {"Long"})
@CXXTemplate(cxx = {"double"},
        java = {"Double"})
@CXXTemplate(cxx = {"uint32_t"},
        java = {"Integer"})
public interface VertexDataColumn<DATA_T> extends FFIPointer {
    @CXXOperator(value = "[]")
    @CXXValue DATA_T get(@FFIConst @CXXReference @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") Vertex<Long> nbr);
}
