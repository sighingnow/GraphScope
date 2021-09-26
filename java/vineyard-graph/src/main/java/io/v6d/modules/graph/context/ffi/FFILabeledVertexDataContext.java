package io.v6d.modules.graph.context.ffi;

import com.alibaba.ffi.*;
import com.alibaba.grape.ds.GSVertexArray;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.stdcxx.StdVector;

import static com.alibaba.grape.utils.CPP_CLASSES_STRINGS.GRAPE_VERTEX;
import static io.v6d.modules.graph.utils.CPP_CLASS.ARROW_FRAGMENT;
import static io.v6d.modules.graph.utils.CPP_CLASS.LABELED_VERTEX_DATA_CONTEXT;
import static io.v6d.modules.graph.utils.CPP_HEADER.*;
import static io.v6d.modules.graph.utils.CPP_JNI_LIBRARY.VINEYARD_JNI_LIBRARY;

@FFIGen(library = VINEYARD_JNI_LIBRARY)
@CXXHead(LABELED_VERTEX_DATA_CONTEXT_H)
@CXXHead(ARROW_FRAGMENT_H)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(LABELED_VERTEX_DATA_CONTEXT)
@CXXTemplate(cxx = {ARROW_FRAGMENT + "<int64_t>", "double"},
        java = {"io.v6d.modules.graph.fragment.ArrowFragment<java.lang.Long>", "java.lang.Double"})
public interface FFILabeledVertexDataContext<FRAG_T, DATA_T> extends FFIPointer {
    @FFINameAlias("GetValue")
    @CXXValue DATA_T getValue(@CXXReference @FFITypeAlias(GRAPE_VERTEX + "<uint64_t>") Vertex<Long> vertex);


    @CXXReference StdVector<GSVertexArray<DATA_T>> data();

    @FFIFactory
    interface Factory<FRAG_T, DATA_T> {
        FFILabeledVertexDataContext<FRAG_T, DATA_T> create(@CXXReference FRAG_T fragment, boolean includeOuter);
    }
}
