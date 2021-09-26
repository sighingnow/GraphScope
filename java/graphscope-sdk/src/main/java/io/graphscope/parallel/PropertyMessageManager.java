package io.v6d.modules.graph.parallel;


import com.alibaba.ffi.*;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.parallel.MessageManagerBase;

import static com.alibaba.grape.utils.CPP_CLASSES_STRINGS.*;
import static com.alibaba.grape.utils.CPP_HEADER_STRINGS.CORE_JAVA_JAVA_MESSAGES_H;
import static io.v6d.modules.graph.utils.CPP_CLASS.ARROW_FRAGMENT;
import static io.v6d.modules.graph.utils.CPP_CLASS.PROPERTY_MESSAGE_MANAGER;
import static io.v6d.modules.graph.utils.CPP_HEADER.ARROW_FRAGMENT_H;
import static io.v6d.modules.graph.utils.CPP_HEADER.PROPERTY_MESSAGE_MANAGER_H;
import static io.v6d.modules.graph.utils.CPP_JNI_LIBRARY.VINEYARD_JNI_LIBRARY;

@FFIGen(library = VINEYARD_JNI_LIBRARY)
@CXXHead({PROPERTY_MESSAGE_MANAGER_H, ARROW_FRAGMENT_H, CORE_JAVA_JAVA_MESSAGES_H})
@CXXHead("cstdint")
@FFITypeAlias(PROPERTY_MESSAGE_MANAGER)
public interface PropertyMessageManager extends MessageManagerBase {

    @CXXTemplate(cxx = {ARROW_FRAGMENT + "<int64_t>", DOUBLE_MSG},
            java = {"io.v6d.modules.graph.fragment.ArrowFragment<java.lang.Long>", "com.alibaba.grape.parallel.message.DoubleMsg"})
    @CXXTemplate(cxx = {ARROW_FRAGMENT + "<int64_t>", LONG_MSG},
            java = {"io.v6d.modules.graph.fragment.ArrowFragment<java.lang.Long>", "com.alibaba.grape.parallel.message.LongMsg"})
    @FFINameAlias("SendMsgThroughIEdges") <FRAG_T, MSG_T>
    void sendMsgThroughIEdges(@CXXReference FRAG_T frag, @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex, int eLabelId, @CXXReference MSG_T msg);

    @CXXTemplate(cxx = {ARROW_FRAGMENT + "<int64_t>", DOUBLE_MSG},
            java = {"io.v6d.modules.graph.fragment.ArrowFragment<java.lang.Long>", "com.alibaba.grape.parallel.message.DoubleMsg"})
    @CXXTemplate(cxx = {ARROW_FRAGMENT + "<int64_t>", LONG_MSG},
            java = {"io.v6d.modules.graph.fragment.ArrowFragment<java.lang.Long>", "com.alibaba.grape.parallel.message.LongMsg"})
    @FFINameAlias("SendMsgThroughOEdges") <FRAG_T, MSG_T>
    void sendMsgThroughOEdges(@CXXReference FRAG_T frag, @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex, int eLabelId, @CXXReference MSG_T msg);


    @CXXTemplate(cxx = {ARROW_FRAGMENT + "<int64_t>", DOUBLE_MSG},
            java = {"io.v6d.modules.graph.fragment.ArrowFragment<java.lang.Long>", "com.alibaba.grape.parallel.message.DoubleMsg"})
    @CXXTemplate(cxx = {ARROW_FRAGMENT + "<int64_t>", LONG_MSG},
            java = {"io.v6d.modules.graph.fragment.ArrowFragment<java.lang.Long>", "com.alibaba.grape.parallel.message.LongMsg"})
    @FFINameAlias("SendMsgThroughEdges") <FRAG_T, MSG_T>
    void sendMsgThroughEdges(@CXXReference FRAG_T frag, @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex, int eLabelId, @CXXReference MSG_T msg);

    @CXXTemplate(cxx = {ARROW_FRAGMENT + "<int64_t>", DOUBLE_MSG},
            java = {"io.v6d.modules.graph.fragment.ArrowFragment<java.lang.Long>",
                    "com.alibaba.grape.parallel.message.DoubleMsg"})
    @CXXTemplate(cxx = {ARROW_FRAGMENT + "<int64_t>", LONG_MSG},
            java = {"io.v6d.modules.graph.fragment.ArrowFragment<java.lang.Long>",
                    "com.alibaba.grape.parallel.message.LongMsg"})
    @FFINameAlias("SyncStateOnOuterVertex") <FRAG_T, MSG_T>
    void syncStateOnOuterVertex(@CXXReference FRAG_T frag, @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex, @CXXReference MSG_T msg);


    @CXXTemplate(cxx = {ARROW_FRAGMENT + "<int64_t>", DOUBLE_MSG},
            java = {"io.v6d.modules.graph.fragment.ArrowFragment<java.lang.Long>",
                    "com.alibaba.grape.parallel.message.DoubleMsg"})
    @CXXTemplate(cxx = {ARROW_FRAGMENT + "<int64_t>", LONG_MSG},
            java = {"io.v6d.modules.graph.fragment.ArrowFragment<java.lang.Long>",
                    "com.alibaba.grape.parallel.message.LongMsg"})
    @FFINameAlias("GetMessage") <FRAG_T, MSG_T>
    boolean getMessage(@CXXReference FRAG_T frag, @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex, @CXXReference MSG_T msg);


}
