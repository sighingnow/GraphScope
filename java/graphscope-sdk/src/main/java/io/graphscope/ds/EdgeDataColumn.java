package io.graphscope.ds;

import com.alibaba.fastffi.*;
import io.graphscope.utils.CppClassName;
import io.graphscope.utils.CppHeaderName;
import io.graphscope.utils.JNILibraryName;

@FFIGen(library = JNILibraryName.VINEYARD_JNI_LIBRARY)
@CXXHead(CppHeaderName.PROPERTY_GRAPH_UTILS_H)
@FFITypeAlias(CppClassName.EDGE_DATA_COLUMN)
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
    @CXXValue DATA_T get(@FFIConst @CXXReference @FFITypeAlias(CppClassName.PROPERTY_NBR_UNIT + "<uint64_t>") PropertyNbrUnit<Long> nbr);
}