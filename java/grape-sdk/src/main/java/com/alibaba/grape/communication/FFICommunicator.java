package com.alibaba.grape.communication;

import com.alibaba.fastffi.*;

import static com.alibaba.grape.utils.CppClassName.*;
import static com.alibaba.grape.utils.CppHeaderName.CORE_JAVA_JAVA_MESSAGES_H;
import static com.alibaba.grape.utils.CppHeaderName.GRAPE_COMMUNICATOR_H;
import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@CXXHead(GRAPE_COMMUNICATOR_H)
@CXXHead(CORE_JAVA_JAVA_MESSAGES_H)
@FFITypeAlias(GRAPE_COMMUNICATOR)
public interface FFICommunicator extends FFIPointer {
    @CXXTemplate(cxx = DOUBLE_MSG, java = "com.alibaba.grape.parallel.message.DoubleMsg")
    @CXXTemplate(cxx = LONG_MSG, java = "com.alibaba.grape.parallel.message.LongMsg")
    @FFINameAlias("Sum") <MSG_T> void sum(@FFIConst @CXXReference MSG_T msgIn, @CXXReference MSG_T msgOut);

    @CXXTemplate(cxx = DOUBLE_MSG, java = "com.alibaba.grape.parallel.message.DoubleMsg")
    @CXXTemplate(cxx = LONG_MSG, java = "com.alibaba.grape.parallel.message.LongMsg")
    @FFINameAlias("Min") <MSG_T> void min(@FFIConst @CXXReference MSG_T msgIn, @CXXReference MSG_T msgOut);

    @CXXTemplate(cxx = DOUBLE_MSG, java = "com.alibaba.grape.parallel.message.DoubleMsg")
    @CXXTemplate(cxx = LONG_MSG, java = "com.alibaba.grape.parallel.message.LongMsg")
    @FFINameAlias("Max") <MSG_T> void max(@FFIConst @CXXReference MSG_T msgIn, @CXXReference MSG_T msgOut);
}
