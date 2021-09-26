package io.v6d.modules.graph.ds;

import com.alibaba.ffi.*;
import com.alibaba.grape.ds.Vertex;

import static io.v6d.modules.graph.utils.CPP_CLASS.PROPERTY_NBR;
import static io.v6d.modules.graph.utils.CPP_HEADER.PROPERTY_GRAPH_UTILS_H;
import static io.v6d.modules.graph.utils.CPP_JNI_LIBRARY.VINEYARD_JNI_LIBRARY;

@FFIGen(library = VINEYARD_JNI_LIBRARY)
@CXXHead(PROPERTY_GRAPH_UTILS_H)
@FFITypeAlias(PROPERTY_NBR)
@CXXTemplate(cxx = {"uint64_t"}, java = {"java.lang.Long"})
public interface PropertyNbr<VID_T> extends FFIPointer, CXXPointerRangeElement<PropertyNbr<VID_T>> {
    @FFINameAlias("neighbor")
    @CXXValue Vertex<VID_T> neighbor();

    @FFINameAlias("get_double")
    double getDouble(int propertyId);

    @FFINameAlias("get_int")
    int getInt(int propertyId);

    @FFINameAlias("get_str")
    @CXXValue FFIByteString getString(int propertyId);

    @CXXOperator("++")
    @CXXReference PropertyNbr<VID_T> inc();

    @CXXOperator("==")
    boolean eq(@CXXReference PropertyNbr<VID_T> rhs);

    @CXXOperator("--")
    @CXXReference PropertyNbr<VID_T> dec();

    @FFIFactory
    interface Factory<VID_T> {
        PropertyNbr<VID_T> create();
    }
}
