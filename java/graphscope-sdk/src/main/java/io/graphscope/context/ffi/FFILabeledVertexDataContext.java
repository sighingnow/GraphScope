package io.graphscope.context.ffi;

import com.alibaba.ffi.*;
import com.alibaba.grape.ds.GSVertexArray;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.stdcxx.StdVector;
import io.graphscope.utils.CppHeaderName;
import io.graphscope.utils.JNILibraryName;

import static com.alibaba.grape.utils.CppClassName.GRAPE_VERTEX;
import static io.graphscope.utils.CppClassName.ARROW_FRAGMENT;
import static io.graphscope.utils.CppClassName.LABELED_VERTEX_DATA_CONTEXT;

@FFIGen(library = JNILibraryName.VINEYARD_JNI_LIBRARY)
@CXXHead(CppHeaderName.LABELED_VERTEX_DATA_CONTEXT_H)
@CXXHead(CppHeaderName.ARROW_FRAGMENT_H)
@CXXHead(CppHeaderName.CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(LABELED_VERTEX_DATA_CONTEXT)
@CXXTemplate(cxx = {ARROW_FRAGMENT + "<int64_t>", "double"},
        java = {"io.graphscope.fragment.ArrowFragment<java.lang.Long>", "java.lang.Double"})
public interface FFILabeledVertexDataContext<FRAG_T, DATA_T> extends FFIPointer {
    @FFINameAlias("GetValue")
    @CXXValue DATA_T getValue(@CXXReference @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") Vertex<Long> vertex);


    @CXXReference StdVector<GSVertexArray<DATA_T>> data();

    @FFIFactory
    interface Factory<FRAG_T, DATA_T> {
        FFILabeledVertexDataContext<FRAG_T, DATA_T> create(@CXXReference FRAG_T fragment, boolean includeOuter);
    }
}
