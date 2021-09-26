package io.graphscope.ds;

import com.alibaba.ffi.*;
import com.alibaba.grape.ds.Vertex;
import io.graphscope.utils.CPP_CLASS;
import io.graphscope.utils.CPP_HEADER;
import io.graphscope.utils.CPP_JNI_LIBRARY;

@FFIGen(library = CPP_JNI_LIBRARY.VINEYARD_JNI_LIBRARY)
@CXXHead(CPP_HEADER.PROPERTY_GRAPH_UTILS_H)
@FFITypeAlias(CPP_CLASS.PROPERTY_NBR_UNIT)
@CXXTemplate(cxx = {"uint64_t"}, java = {"java.lang.Long"})
public interface PropertyNbrUnit<VID_T> extends FFIPointer, CXXPointerRangeElement<PropertyNbrUnit<VID_T>> {
    @FFIGetter
    VID_T vid();

    @FFIGetter
    long eid();

    @FFINameAlias("get_neighbor")
    @CXXValue Vertex<VID_T> getNeighbor();
}
