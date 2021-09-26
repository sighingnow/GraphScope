package io.graphscope.context.ffi;

import com.alibaba.ffi.*;
import io.graphscope.column.DoubleColumn;
import io.graphscope.column.IntColumn;
import io.graphscope.column.LongColumn;
import io.graphscope.fragment.ArrowFragment;
import io.graphscope.utils.CPP_HEADER;
import io.graphscope.utils.CPP_JNI_LIBRARY;
import io.graphscope.context.ContextDataType;
import io.graphscope.ds.stdcxx.StdSharedPtr;

import static io.graphscope.utils.CPP_CLASS.ARROW_FRAGMENT;
import static io.graphscope.utils.CPP_CLASS.LABELED_VERTEX_PROPERTY_CONTEXT;

@FFIGen(library = CPP_JNI_LIBRARY.VINEYARD_JNI_LIBRARY)
@CXXHead(CPP_HEADER.CORE_JAVA_TYPE_ALIAS_H)
@CXXHead(CPP_HEADER.LABELED_VERTEX_PROPERTY_CONTEXT_H)
@CXXHead(CPP_HEADER.ARROW_FRAGMENT_H)
@FFITypeAlias(LABELED_VERTEX_PROPERTY_CONTEXT)
@CXXTemplate(cxx = {ARROW_FRAGMENT + "<int64_t>"},
        java = {"io.graphscope.fragment.ArrowFragment<java.lang.Long>"})
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
