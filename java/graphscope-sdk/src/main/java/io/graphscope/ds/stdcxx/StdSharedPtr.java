package io.graphscope.ds.stdcxx;

import com.alibaba.fastffi.*;
import io.graphscope.utils.CppHeaderName;
import io.graphscope.utils.JNILibraryName;


@FFIGen(library = JNILibraryName.VINEYARD_JNI_LIBRARY)
@CXXHead(system = "memory")
@CXXHead(CppHeaderName.ARROW_FRAGMENT_H)
@CXXHead(CppHeaderName.CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias("std::shared_ptr")
@CXXTemplate(cxx = "gs::DoubleColumn<gs::ArrowFragmentDefault<int64_t>>",
        java = "io.graphscope.column.DoubleColumn<io.graphscope.fragment.ArrowFragment<java.lang.Long>>")
@CXXTemplate(cxx = "gs::IntColumn<gs::ArrowFragmentDefault<int64_t>>",
        java = "io.graphscope.column.IntColumn<io.graphscope.fragment.ArrowFragment<java.lang.Long>>")
@CXXTemplate(cxx = "gs::LongColumn<gs::ArrowFragmentDefault<int64_t>>",
        java = "io.graphscope.column.LongColumn<io.graphscope.fragment.ArrowFragment<java.lang.Long>>")
@CXXTemplate(cxx = "gs::DoubleColumn<gs::ArrowProjectedFragment<int64_t,uint64_t,grape::EmptyType,int64_t>>",
        java = "io.graphscope.column.DoubleColumn<com.alibaba.grape.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,com.alibaba.grape.ds.EmptyType,java.lang.Long>>")
@CXXTemplate(cxx = "gs::IntColumn<gs::ArrowProjectedFragment<int64_t,uint64_t,grape::EmptyType,int64_t>>",
        java = "io.graphscope.column.IntColumn<com.alibaba.grape.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,com.alibaba.grape.ds.EmptyType,java.lang.Long>>")
@CXXTemplate(cxx = "gs::LongColumn<gs::ArrowProjectedFragment<int64_t,uint64_t,grape::EmptyType,int64_t>>",
        java = "io.graphscope.column.LongColumn<com.alibaba.grape.fragment.ArrowProjectedFragment<java.lang.Long,java.lang.Long,com.alibaba.grape.ds.EmptyType,java.lang.Long>>")
public interface StdSharedPtr<T extends FFIPointer> extends FFIPointer {
    //& will return the pointer of T.
    //shall be cxxvalue?
    T get();
}
