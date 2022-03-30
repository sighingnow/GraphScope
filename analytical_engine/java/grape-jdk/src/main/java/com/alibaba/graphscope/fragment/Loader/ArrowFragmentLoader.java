package com.alibaba.graphscope.fragment.Loader;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;
import com.alibaba.graphscope.utils.JNILibraryName;

@FFIGen(library = JNILibraryName.JNI_LIBRARY_NAME)
@CXXHead(CppHeaderName.ARROW_FRAGMENT_LOADER_H)
@CXXHead(CppHeaderName.CORE_JAVA_TYPE_ALIAS_H)
@CXXHead(system = "stdint.h")
@FFITypeAlias(CppClassName.ARROW_FRAGMENT_LOADER)

public interface ArrowFragmentLoader extends FFIPointer {

    @FFINameAlias("GetJavaLoaderInvoker")
    JavaLoaderInvoker getJavaLoaderInvoker();

}
