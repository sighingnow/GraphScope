package com.alibaba.grape.parallel;

import com.alibaba.fastffi.*;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.fragment.ArrowProjectedFragment;
import com.alibaba.grape.fragment.ImmutableEdgecutFragment;

import static com.alibaba.grape.utils.CppClassName.*;
import static com.alibaba.grape.utils.CppHeaderName.*;
import static com.alibaba.grape.utils.JNILibraryName.GRAPE_JNI_LIBRARY;

@FFIGen(library = GRAPE_JNI_LIBRARY)
@FFITypeAlias(GRAPE_DEFAULT_MESSAGE_MANAGER)
@CXXHead({GRAPE_ADJ_LIST_H, GRAPE_PARALLEL_DEFAULT_MESSAGE_MANAGER_H,
        GRAPE_FRAGMENT_IMMUTABLE_EDGECUT_FRAGMENT_H,
        ARROW_PROJECTED_FRAGMENT_H,
        CORE_JAVA_JAVA_MESSAGES_H})
public interface DefaultMessageManager extends MessageManagerBase {
//    @CXXTemplate(cxx = DOUBLE_MSG, java = "com.alibaba.grape.parallel.message.DoubleMsg")
//    @CXXTemplate(cxx = LONG_MSG, java = "com.alibaba.grape.parallel.message.LongMsg")
//    @FFINameAlias("Sum") <MSG_T> void sum(@FFIConst @CXXReference MSG_T msgIn, @CXXReference MSG_T msgOut);
//
//    @CXXTemplate(cxx = DOUBLE_MSG, java = "com.alibaba.grape.parallel.message.DoubleMsg")
//    @CXXTemplate(cxx = LONG_MSG, java = "com.alibaba.grape.parallel.message.LongMsg")
//    @FFINameAlias("Min") <MSG_T> void min(@FFIConst @CXXReference MSG_T msgIn, @CXXReference MSG_T msgOut);
//
//    @CXXTemplate(cxx = DOUBLE_MSG, java = "com.alibaba.grape.parallel.message.DoubleMsg")
//    @CXXTemplate(cxx = LONG_MSG, java = "com.alibaba.grape.parallel.message.LongMsg")
//    @FFINameAlias("Max") <MSG_T> void max(@FFIConst @CXXReference MSG_T msgIn, @CXXReference MSG_T msgOut);

    @CXXTemplate(cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", DOUBLE_MSG},
            java = {"com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
                    "com.alibaba.grape.parallel.message.DoubleMsg"})
    @CXXTemplate(cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", LONG_MSG},
            java = {"com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
                    "com.alibaba.grape.parallel.message.LongMsg"})
    @FFINameAlias("GetMessage") <FRAG_T extends ImmutableEdgecutFragment, MSG_T> boolean
    getMessage(@CXXReference FRAG_T frag, @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex, @CXXReference MSG_T msg);

    @CXXTemplate(cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>", DOUBLE_MSG},
            java = {"com.alibaba.grape.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
                    "com.alibaba.grape.parallel.message.DoubleMsg"})
    @CXXTemplate(cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>", LONG_MSG},
            java = {"com.alibaba.grape.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
                    "com.alibaba.grape.parallel.message.LongMsg"})
    @FFINameAlias("GetMessage") <FRAG_T extends ArrowProjectedFragment, MSG_T> boolean
    getMessage(@CXXReference FRAG_T frag, @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex, @CXXReference MSG_T msg);

    @CXXTemplate(cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", DOUBLE_MSG},
            java = {"com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
                    "com.alibaba.grape.parallel.message.DoubleMsg"})
    @CXXTemplate(cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", LONG_MSG},
            java = {"com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
                    "com.alibaba.grape.parallel.message.LongMsg"})
    @FFINameAlias("SyncStateOnOuterVertex") <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void
    syncStateOnOuterVertex(@CXXReference FRAG_T frag, @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex, @CXXReference MSG_T msg);

    @CXXTemplate(cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>", DOUBLE_MSG},
            java = {"com.alibaba.grape.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
                    "com.alibaba.grape.parallel.message.DoubleMsg"})
    @CXXTemplate(cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>", LONG_MSG},
            java = {"com.alibaba.grape.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
                    "com.alibaba.grape.parallel.message.LongMsg"})
    @FFINameAlias("SyncStateOnOuterVertex") <FRAG_T extends ArrowProjectedFragment, MSG_T> void
    syncStateOnOuterVertex(@CXXReference FRAG_T frag, @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex, @CXXReference MSG_T msg);


    @CXXTemplate(cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", DOUBLE_MSG},
            java = {"com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
                    "com.alibaba.grape.parallel.message.DoubleMsg"})
    @CXXTemplate(cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", LONG_MSG},
            java = {"com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
                    "com.alibaba.grape.parallel.message.LongMsg"})
    @FFINameAlias("SendMsgThroughOEdges") <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void
    sendMsgThroughOEdges(@CXXReference FRAG_T frag, @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex, @CXXReference MSG_T msg);

    @CXXTemplate(cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>", DOUBLE_MSG},
            java = {"com.alibaba.grape.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
                    "com.alibaba.grape.parallel.message.DoubleMsg"})
    @CXXTemplate(cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>", LONG_MSG},
            java = {"com.alibaba.grape.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
                    "com.alibaba.grape.parallel.message.LongMsg"})
    @FFINameAlias("SendMsgThroughOEdges") <FRAG_T extends ArrowProjectedFragment, MSG_T> void
    sendMsgThroughOEdges(@CXXReference FRAG_T frag, @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex, @CXXReference MSG_T msg);

    @CXXTemplate(cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", DOUBLE_MSG},
            java = {"com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
                    "com.alibaba.grape.parallel.message.DoubleMsg"})
    @CXXTemplate(cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", LONG_MSG},
            java = {"com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
                    "com.alibaba.grape.parallel.message.LongMsg"})
    @FFINameAlias("SendMsgThroughIEdges") <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void
    sendMsgThroughIEdges(@CXXReference FRAG_T frag, @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex, @CXXReference MSG_T msg);

    @CXXTemplate(cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>", DOUBLE_MSG},
            java = {"com.alibaba.grape.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
                    "com.alibaba.grape.parallel.message.DoubleMsg"})
    @CXXTemplate(cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>", LONG_MSG},
            java = {"com.alibaba.grape.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
                    "com.alibaba.grape.parallel.message.LongMsg"})
    @FFINameAlias("SendMsgThroughIEdges") <FRAG_T extends ArrowProjectedFragment, MSG_T> void
    sendMsgThroughIEdges(@CXXReference FRAG_T frag, @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex, @CXXReference MSG_T msg);


    @CXXTemplate(cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", DOUBLE_MSG},
            java = {"com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
                    "com.alibaba.grape.parallel.message.DoubleMsg"})
    @CXXTemplate(cxx = {GRAPE_IMMUTABLE_FRAGMENT + "<jlong,uint64_t,jlong,jdouble>", LONG_MSG},
            java = {"com.alibaba.grape.fragment.ImmutableEdgecutFragment<Long,Long,Long,Double>",
                    "com.alibaba.grape.parallel.message.LongMsg"})
    @FFINameAlias("SendMsgThroughEdges") <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void
    sendMsgThroughEdges(@CXXReference FRAG_T frag, @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex, @CXXReference MSG_T msg);

    @CXXTemplate(cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>", DOUBLE_MSG},
            java = {"com.alibaba.grape.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
                    "com.alibaba.grape.parallel.message.DoubleMsg"})
    @CXXTemplate(cxx = {ARROW_PROJECTED_FRAGMENT + "<int64_t,uint64_t,double,int64_t>", LONG_MSG},
            java = {"com.alibaba.grape.fragment.ArrowProjectedFragment<Long,Long,Double,Long>",
                    "com.alibaba.grape.parallel.message.LongMsg"})
    @FFINameAlias("SendMsgThroughEdges") <FRAG_T extends ArrowProjectedFragment, MSG_T> void
    sendMsgThroughEdges(@CXXReference FRAG_T frag, @CXXReference @FFITypeAlias(GRAPE_LONG_VERTEX) Vertex<Long> vertex, @CXXReference MSG_T msg);

}
