package io.graphscope.ds;

import com.alibaba.fastffi.*;
import com.alibaba.grape.ds.Vertex;
import io.graphscope.utils.CppClassName;
import io.graphscope.utils.CppHeaderName;
import io.graphscope.utils.JNILibraryName;

@FFIGen(library = JNILibraryName.VINEYARD_JNI_LIBRARY)
@CXXHead(CppHeaderName.CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(CppClassName.PROPERTY_NBR_UNIT)
@CXXTemplate(cxx = {"uint64_t"}, java = {"java.lang.Long"})
public interface PropertyNbrUnit<VID_T> extends FFIPointer, CXXPointerRangeElement<PropertyNbrUnit<VID_T>> {
    @FFIGetter
    VID_T vid();

    @FFIGetter
    long eid();

    @FFINameAlias("get_neighbor")
    @CXXValue Vertex<VID_T> getNeighbor();
}
