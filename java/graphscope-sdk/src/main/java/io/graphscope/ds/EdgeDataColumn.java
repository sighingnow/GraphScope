package io.graphscope.ds;

import com.alibaba.ffi.*;
import io.graphscope.utils.CPP_CLASS;
import io.graphscope.utils.CPP_HEADER;
import io.graphscope.utils.CPP_JNI_LIBRARY;

@FFIGen(library = CPP_JNI_LIBRARY.VINEYARD_JNI_LIBRARY)
@CXXHead(CPP_HEADER.PROPERTY_GRAPH_UTILS_H)
@FFITypeAlias(CPP_CLASS.EDGE_DATA_COLUMN)
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
    @CXXValue DATA_T get(@FFIConst @CXXReference @FFITypeAlias(CPP_CLASS.PROPERTY_NBR_UNIT + "<uint64_t>") PropertyNbrUnit<Long> nbr);
}