package io.v6d.modules.graph.ds;

import com.alibaba.ffi.*;

import static io.v6d.modules.graph.utils.CPP_CLASS.EDGE_DATA_COLUMN;
import static io.v6d.modules.graph.utils.CPP_CLASS.PROPERTY_NBR_UNIT;
import static io.v6d.modules.graph.utils.CPP_HEADER.PROPERTY_GRAPH_UTILS_H;
import static io.v6d.modules.graph.utils.CPP_JNI_LIBRARY.VINEYARD_JNI_LIBRARY;

@FFIGen(library = VINEYARD_JNI_LIBRARY)
@CXXHead(PROPERTY_GRAPH_UTILS_H)
@FFITypeAlias(EDGE_DATA_COLUMN)
@CXXTemplate(cxx = {"uint64_t"},
        java = {"Long"})
@CXXTemplate(cxx = {"double"},
        java = {"Double"})
@CXXTemplate(cxx = {"uint32_t"},
        java = {"Integer"})
/**
 * VID_T is default uint64_t
 */
public interface EdgeDataColumn<DATA_T> extends FFIPointer {

    @CXXOperator(value = "[]")
    @CXXValue DATA_T get(@FFIConst @CXXReference @FFITypeAlias(PROPERTY_NBR_UNIT + "<uint64_t>") PropertyNbrUnit<Long> nbr);
}