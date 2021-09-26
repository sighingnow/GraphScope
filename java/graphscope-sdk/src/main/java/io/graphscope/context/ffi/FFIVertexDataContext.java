package io.graphscope.context.ffi;

import com.alibaba.ffi.*;
import com.alibaba.grape.ds.GSVertexArray;
import io.graphscope.utils.CPP_HEADER;

import static com.alibaba.grape.utils.CPP_CLASSES_STRINGS.ARROW_PROJECTED_FRAGMENT;
import static com.alibaba.grape.utils.CPP_CLASSES_STRINGS.GRAPE_EMPTY_TYPE;
import static com.alibaba.grape.utils.CPP_HEADER_STRINGS.ARROW_PROJECTED_FRAGMENT_H;
import static io.graphscope.utils.CPP_CLASS.VERTEX_DATA_CONTEXT;
import static io.graphscope.utils.CPP_JNI_LIBRARY.VINEYARD_JNI_LIBRARY;

@FFIGen(library = VINEYARD_JNI_LIBRARY)
@CXXHead(CPP_HEADER.VERTEX_DATA_CONTEXT_H)
@CXXHead(ARROW_PROJECTED_FRAGMENT_H)
@FFITypeAlias(VERTEX_DATA_CONTEXT)
@CXXTemplate(cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t," + GRAPE_EMPTY_TYPE + ",int64_t>", "double"},
        java = {"com.alibaba.grape.fragment.ArrowProjectedFragment<Long,Long,com.alibaba.grape.ds.EmptyType,Long>", "java.lang.Double"})
@CXXTemplate(cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>", "double"},
        java = {"com.alibaba.grape.fragment.ArrowProjectedFragment<Long,Long,Double,Long>", "java.lang.Double"})
@CXXTemplate(cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>", "int64_t"},
        java = {"com.alibaba.grape.fragment.ArrowProjectedFragment<Long,Long,Double,Long>", "java.lang.Long"})
public interface FFIVertexDataContext<FRAG_T, DATA_T> extends FFIPointer {

    @CXXReference GSVertexArray<DATA_T> data();

    @FFIFactory
    interface Factory<FRAG_T, DATA_T> {
        FFIVertexDataContext<FRAG_T, DATA_T> create(@CXXReference FRAG_T fragment, boolean includeOuter);
    }
}
