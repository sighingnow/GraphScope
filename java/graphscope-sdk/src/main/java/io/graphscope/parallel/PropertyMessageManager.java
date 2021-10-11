package io.graphscope.parallel;


import com.alibaba.ffi.*;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.parallel.MessageManagerBase;
import io.graphscope.utils.CppClassName;
import io.graphscope.utils.CppHeaderName;
import io.graphscope.utils.JNILibraryName;

import static com.alibaba.grape.utils.CppClassName.*;
import static com.alibaba.grape.utils.CppHeaderName.CORE_JAVA_JAVA_MESSAGES_H;

@FFIGen(library = JNILibraryName.VINEYARD_JNI_LIBRARY)
@CXXHead({CppHeaderName.PROPERTY_MESSAGE_MANAGER_H, CppHeaderName.ARROW_FRAGMENT_H, CORE_JAVA_JAVA_MESSAGES_H})
@CXXHead("cstdint")
@FFITypeAlias(CppClassName.PROPERTY_MESSAGE_MANAGER)
public interface PropertyMessageManager extends MessageManagerBase {

    @CXXTemplate(cxx = {CppClassName.ARROW_FRAGMENT + "<int64_t>", DOUBLE_MSG},
            java = {"io.graphscope.fragment.ArrowFragment<java.lang.Long>", "com.alibaba.grape.parallel.message.DoubleMsg"})
    @CXXTemplate(cxx = {CppClassName.ARROW_FRAGMENT + "<int64_t>", LONG_MSG},
            java = {"io.graphscope.fragment.ArrowFragment<java.lang.Long>", "com.alibaba.grape.parallel.message.LongMsg"})
    @FFINameAlias("SendMsgThroughIEdges") <FRAG_T, MSG_T>
    void sendMsgThroughIEdges(@CXXReference FRAG_T frag, @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex, int eLabelId, @CXXReference MSG_T msg);

    @CXXTemplate(cxx = {CppClassName.ARROW_FRAGMENT + "<int64_t>", DOUBLE_MSG},
            java = {"io.graphscope.fragment.ArrowFragment<java.lang.Long>", "com.alibaba.grape.parallel.message.DoubleMsg"})
    @CXXTemplate(cxx = {CppClassName.ARROW_FRAGMENT + "<int64_t>", LONG_MSG},
            java = {"io.graphscope.fragment.ArrowFragment<java.lang.Long>", "com.alibaba.grape.parallel.message.LongMsg"})
    @FFINameAlias("SendMsgThroughOEdges") <FRAG_T, MSG_T>
    void sendMsgThroughOEdges(@CXXReference FRAG_T frag, @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex, int eLabelId, @CXXReference MSG_T msg);


    @CXXTemplate(cxx = {CppClassName.ARROW_FRAGMENT + "<int64_t>", DOUBLE_MSG},
            java = {"io.graphscope.fragment.ArrowFragment<java.lang.Long>", "com.alibaba.grape.parallel.message.DoubleMsg"})
    @CXXTemplate(cxx = {CppClassName.ARROW_FRAGMENT + "<int64_t>", LONG_MSG},
            java = {"io.graphscope.fragment.ArrowFragment<java.lang.Long>", "com.alibaba.grape.parallel.message.LongMsg"})
    @FFINameAlias("SendMsgThroughEdges") <FRAG_T, MSG_T>
    void sendMsgThroughEdges(@CXXReference FRAG_T frag, @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex, int eLabelId, @CXXReference MSG_T msg);

    @CXXTemplate(cxx = {CppClassName.ARROW_FRAGMENT + "<int64_t>", DOUBLE_MSG},
            java = {"io.graphscope.fragment.ArrowFragment<java.lang.Long>",
                    "com.alibaba.grape.parallel.message.DoubleMsg"})
    @CXXTemplate(cxx = {CppClassName.ARROW_FRAGMENT + "<int64_t>", LONG_MSG},
            java = {"io.graphscope.fragment.ArrowFragment<java.lang.Long>",
                    "com.alibaba.grape.parallel.message.LongMsg"})
    @FFINameAlias("SyncStateOnOuterVertex") <FRAG_T, MSG_T>
    void syncStateOnOuterVertex(@CXXReference FRAG_T frag, @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex, @CXXReference MSG_T msg);


    @CXXTemplate(cxx = {CppClassName.ARROW_FRAGMENT + "<int64_t>", DOUBLE_MSG},
            java = {"io.graphscope.fragment.ArrowFragment<java.lang.Long>",
                    "com.alibaba.grape.parallel.message.DoubleMsg"})
    @CXXTemplate(cxx = {CppClassName.ARROW_FRAGMENT + "<int64_t>", LONG_MSG},
            java = {"io.graphscope.fragment.ArrowFragment<java.lang.Long>",
                    "com.alibaba.grape.parallel.message.LongMsg"})
    @FFINameAlias("GetMessage") <FRAG_T, MSG_T>
    boolean getMessage(@CXXReference FRAG_T frag, @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex, @CXXReference MSG_T msg);


}
