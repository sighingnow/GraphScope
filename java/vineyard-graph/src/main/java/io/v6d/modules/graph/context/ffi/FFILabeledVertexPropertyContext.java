package io.v6d.modules.graph.context.ffi;

import com.alibaba.ffi.*;
import io.v6d.modules.graph.column.DoubleColumn;
import io.v6d.modules.graph.column.IntColumn;
import io.v6d.modules.graph.column.LongColumn;
import io.v6d.modules.graph.context.ContextDataType;
import io.v6d.modules.graph.ds.stdcxx.StdSharedPtr;

import static io.v6d.modules.graph.utils.CPP_CLASS.ARROW_FRAGMENT;
import static io.v6d.modules.graph.utils.CPP_CLASS.LABELED_VERTEX_PROPERTY_CONTEXT;
import static io.v6d.modules.graph.utils.CPP_HEADER.*;
import static io.v6d.modules.graph.utils.CPP_JNI_LIBRARY.VINEYARD_JNI_LIBRARY;

@FFIGen(library = VINEYARD_JNI_LIBRARY)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@CXXHead(LABELED_VERTEX_PROPERTY_CONTEXT_H)
@CXXHead(ARROW_FRAGMENT_H)
@FFITypeAlias(LABELED_VERTEX_PROPERTY_CONTEXT)
@CXXTemplate(cxx = {ARROW_FRAGMENT + "<int64_t>"},
        java = {"io.v6d.modules.graph.fragment.ArrowFragment<java.lang.Long>"})
public interface FFILabeledVertexPropertyContext<FRAG_T> extends FFIPointer {
    @FFINameAlias("add_column")
    long addColumn(int labelId, @CXXReference FFIByteString name, @CXXValue ContextDataType contextDataType);

    @FFINameAlias("get_typed_column<double>")
    @CXXValue StdSharedPtr<DoubleColumn<FRAG_T>> getDoubleColumn(int labelId, long index);

    @FFINameAlias("get_typed_column<double>")
    @CXXValue StdSharedPtr<DoubleColumn<FRAG_T>> getDoubleColumn(int labelId, @CXXReference FFIByteString name);

    @FFINameAlias("get_typed_column<uint32_t>")
    @CXXValue StdSharedPtr<IntColumn<FRAG_T>> getIntColumn(int labelId, long index);

    @FFINameAlias("get_typed_column<uint32_t>")
    @CXXValue StdSharedPtr<IntColumn<FRAG_T>> getIntColumn(int labelId, @CXXReference FFIByteString name);

    @FFINameAlias("get_typed_column<uint64_t>")
    @CXXValue StdSharedPtr<LongColumn<FRAG_T>> getLongColumn(int labelId, long index);

    @FFINameAlias("get_typed_column<uint64_t>")
    @CXXValue StdSharedPtr<LongColumn<FRAG_T>> getLongColumn(int labelId, @CXXReference FFIByteString name);

    @FFIFactory
    interface Factory<FRAG_T> {
        FFILabeledVertexPropertyContext<FRAG_T> create(@CXXReference FRAG_T fragment);
    }
}
