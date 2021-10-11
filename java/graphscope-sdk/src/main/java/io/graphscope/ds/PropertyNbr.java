package io.graphscope.ds;

import com.alibaba.ffi.*;
import com.alibaba.grape.ds.Vertex;
import io.graphscope.utils.CppClassName;
import io.graphscope.utils.CppHeaderName;
import io.graphscope.utils.JNILibraryName;

@FFIGen(library = JNILibraryName.VINEYARD_JNI_LIBRARY)
@CXXHead(CppHeaderName.PROPERTY_GRAPH_UTILS_H)
@FFITypeAlias(CppClassName.PROPERTY_NBR)
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
