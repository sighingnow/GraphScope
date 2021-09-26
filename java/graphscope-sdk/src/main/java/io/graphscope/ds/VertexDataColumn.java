package io.graphscope.ds;

import com.alibaba.ffi.*;
import com.alibaba.grape.ds.Vertex;
import io.graphscope.utils.CPP_CLASS;
import io.graphscope.utils.CPP_HEADER;
import io.graphscope.utils.CPP_JNI_LIBRARY;

import static com.alibaba.grape.utils.CPP_CLASSES_STRINGS.GRAPE_VERTEX;

@FFIGen(library = CPP_JNI_LIBRARY.VINEYARD_JNI_LIBRARY)
@CXXHead(CPP_HEADER.PROPERTY_GRAPH_UTILS_H)
@FFITypeAlias(CPP_CLASS.VERTEX_DATA_COLUMN)
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
