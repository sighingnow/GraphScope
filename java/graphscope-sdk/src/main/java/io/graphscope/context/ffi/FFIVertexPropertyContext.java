package io.graphscope.context.ffi;

import com.alibaba.fastffi.*;
import io.graphscope.column.DoubleColumn;
import io.graphscope.column.IntColumn;
import io.graphscope.column.LongColumn;
import io.graphscope.utils.CppHeaderName;
import io.graphscope.context.ContextDataType;
import io.graphscope.ds.stdcxx.StdSharedPtr;

import static com.alibaba.grape.utils.CppClassName.ARROW_PROJECTED_FRAGMENT;
import static com.alibaba.grape.utils.CppHeaderName.ARROW_PROJECTED_FRAGMENT_H;
import static com.alibaba.grape.utils.CppHeaderName.GRAPE_TYPES_H;
import static io.graphscope.utils.CppClassName.VERTEX_PROPERTY_CONTEXT;
import static io.graphscope.utils.JNILibraryName.VINEYARD_JNI_LIBRARY;

@FFIGen(library = VINEYARD_JNI_LIBRARY)
@CXXHead(system = "cstdint")
@CXXHead(CppHeaderName.CORE_JAVA_TYPE_ALIAS_H)
@CXXHead(CppHeaderName.VERTEX_PROPERTY_CONTEXT_H)
@CXXHead(ARROW_PROJECTED_FRAGMENT_H)
@CXXHead(GRAPE_TYPES_H)
@FFITypeAlias(VERTEX_PROPERTY_CONTEXT)
@CXXTemplate(cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,grape::EmptyType,int64_t>"},
        java = {"com.alibaba.grape.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,com.alibaba.grape.ds.EmptyType,java.lang.Long>"})
public interface FFIVertexPropertyContext<FRAG_T> extends FFIPointer {
    @FFINameAlias("add_column")
    long addColumn(@CXXReference FFIByteString name, @CXXValue ContextDataType contextDataType);

    @FFINameAlias("get_typed_column<double>")
    @CXXValue StdSharedPtr<DoubleColumn<FRAG_T>> getDoubleColumn(long index);

    @FFINameAlias("get_typed_column<double>")
    @CXXValue StdSharedPtr<DoubleColumn<FRAG_T>> getDoubleColumn(@CXXReference FFIByteString name);

    @FFINameAlias("get_typed_column<uint32_t>")
    @CXXValue StdSharedPtr<IntColumn<FRAG_T>> getIntColumn(long index);

    @FFINameAlias("get_typed_column<uint32_t>")
    @CXXValue StdSharedPtr<IntColumn<FRAG_T>> getIntColumn(@CXXReference FFIByteString name);

    @FFINameAlias("get_typed_column<uint64_t>")
    @CXXValue StdSharedPtr<LongColumn<FRAG_T>> getLongColumn(long index);

    @FFINameAlias("get_typed_column<uint64_t>")
    @CXXValue StdSharedPtr<LongColumn<FRAG_T>> getLongColumn(@CXXReference FFIByteString name);

    @FFIFactory
    interface Factory<FRAG_T> {
        FFIVertexPropertyContext<FRAG_T> create(@CXXReference FRAG_T fragment);
    }
}
