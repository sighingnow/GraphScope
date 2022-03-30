package com.alibaba.graphscope.fragment.Loader;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFIVector;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;
import com.alibaba.graphscope.utils.JNILibraryName;

@FFIGen(library = JNILibraryName.JNI_LIBRARY_NAME)
@CXXHead(CppHeaderName.JAVA_LOADER_INVOKER_H)
@CXXHead(CppHeaderName.CORE_JAVA_TYPE_ALIAS_H)
@CXXHead(system = "stdint.h")
@FFITypeAlias(CppClassName.JAVA_LOADER_INVOKER)
public interface JavaLoaderInvoker extends FFIPointer {

    @FFINameAlias("WorkerId")
    int workerId();

    @FFINameAlias("WorkerNum")
    int workerNum();

    @FFINameAlias("LoadingThreadNum")
    int loadingThreadNum();

    @FFINameAlias("SetTypeInfoInt")
    void setTypeInfoInt(int infoInt);

    @FFINameAlias("GetOids")
    @CXXReference FFIVector<Byte> getOids();

    @FFINameAlias("GetVdatas")
    @CXXReference FFIVector<FFIVector<Byte>> getVdatas();

    @FFINameAlias("GetEdgeSrcs")
    @CXXReference  FFIVector<FFIVector<Byte>> getEdgeSrcs();

    @FFINameAlias("GetEdgeDsts")
    @CXXReference FFIVector<FFIVector<Byte>> getEdgeDsts();

    @FFINameAlias("GetEdgeDatas")
    @CXXReference FFIVector<FFIVector<Byte>> getEdgeDatas();

    @FFINameAlias("GetOidOffsets")
    @CXXReference  FFIVector<FFIVector<Integer>> getOidOffsets();

    @FFINameAlias("GetVdataOffsets")
    @CXXReference  FFIVector<FFIVector<Integer>> getVdataOffsets();

    @FFINameAlias("GetEdgeSrcOffsets")
    @CXXReference  FFIVector<FFIVector<Integer>> getEdgeSrcOffsets();

    @FFINameAlias("GetEdgeDstOffsets")
    @CXXReference  FFIVector<FFIVector<Integer>> getEdgeDstOffsets();

    @FFINameAlias("GetEdgeDataOffsets")
    @CXXReference  FFIVector<FFIVector<Integer>> getEdgeDataOffsets();

}
