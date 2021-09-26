package io.v6d.modules.graph.ds.stdcxx;

import com.alibaba.ffi.*;

import static io.v6d.modules.graph.utils.CPP_HEADER.ARROW_FRAGMENT_H;
import static io.v6d.modules.graph.utils.CPP_HEADER.CORE_JAVA_TYPE_ALIAS_H;
import static io.v6d.modules.graph.utils.CPP_JNI_LIBRARY.VINEYARD_JNI_LIBRARY;


@FFIGen(library = VINEYARD_JNI_LIBRARY)
@CXXHead(system = "memory")
@CXXHead(ARROW_FRAGMENT_H)
@CXXHead(CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias("std::shared_ptr")
@CXXTemplate(cxx = "gs::DoubleColumn<vineyard::ArrowFragmentDefault<int64_t>>",
        java = "io.v6d.modules.graph.column.DoubleColumn<io.v6d.modules.graph.fragment.ArrowFragment<java.lang.Long>>")
@CXXTemplate(cxx = "gs::IntColumn<vineyard::ArrowFragmentDefault<int64_t>>",
        java = "io.v6d.modules.graph.column.IntColumn<io.v6d.modules.graph.fragment.ArrowFragment<java.lang.Long>>")
@CXXTemplate(cxx = "gs::LongColumn<vineyard::ArrowFragmentDefault<int64_t>>",
        java = "io.v6d.modules.graph.column.LongColumn<io.v6d.modules.graph.fragment.ArrowFragment<java.lang.Long>>")
@CXXTemplate(cxx = "gs::DoubleColumn<gs::ArrowProjectedFragment<int64_t,uint64_t,grape::EmptyType,int64_t>>",
        java = "io.v6d.modules.graph.column.DoubleColumn<com.alibaba.grape.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,com.alibaba.grape.ds.EmptyType,java.lang.Long>>")
@CXXTemplate(cxx = "gs::IntColumn<gs::ArrowProjectedFragment<int64_t,uint64_t,grape::EmptyType,int64_t>>",
        java = "io.v6d.modules.graph.column.IntColumn<com.alibaba.grape.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,com.alibaba.grape.ds.EmptyType,java.lang.Long>>")
@CXXTemplate(cxx = "gs::LongColumn<gs::ArrowProjectedFragment<int64_t,uint64_t,grape::EmptyType,int64_t>>",
        java = "io.v6d.modules.graph.column.LongColumn<com.alibaba.grape.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,com.alibaba.grape.ds.EmptyType,java.lang.Long>>")
public interface StdSharedPtr<T extends FFIPointer> extends FFIPointer {
    //& will return the pointer of T.
    //shall be cxxvalue?
    T get();
}
