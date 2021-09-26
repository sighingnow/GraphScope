package io.v6d.modules.graph.ds;

import com.alibaba.ffi.*;
import com.alibaba.grape.ds.Vertex;

import static io.v6d.modules.graph.utils.CPP_CLASS.PROPERTY_NBR_UNIT;
import static io.v6d.modules.graph.utils.CPP_HEADER.PROPERTY_GRAPH_UTILS_H;
import static io.v6d.modules.graph.utils.CPP_JNI_LIBRARY.VINEYARD_JNI_LIBRARY;

@FFIGen(library = VINEYARD_JNI_LIBRARY)
@CXXHead(PROPERTY_GRAPH_UTILS_H)
@FFITypeAlias(PROPERTY_NBR_UNIT)
@CXXTemplate(cxx = {"uint64_t"}, java = {"java.lang.Long"})
public interface PropertyNbrUnit<VID_T> extends FFIPointer, CXXPointerRangeElement<PropertyNbrUnit<VID_T>> {
    @FFIGetter
    VID_T vid();

    @FFIGetter
    long eid();

    @FFINameAlias("get_neighbor")
    @CXXValue Vertex<VID_T> getNeighbor();
}
