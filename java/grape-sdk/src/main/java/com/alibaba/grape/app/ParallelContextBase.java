package com.alibaba.grape.app;

import com.alibaba.fastffi.FFIByteString;
import com.alibaba.grape.fragment.ImmutableEdgecutFragment;
import com.alibaba.grape.parallel.ParallelMessageManager;
import com.alibaba.grape.stdcxx.StdVector;

/**
 * @param <OID_T>   original id type
 * @param <VID_T>   vertex id type
 * @param <VDATA_T> vertex data type
 * @param <EDATA_T> edge data type
 */
public interface ParallelContextBase<OID_T, VID_T, VDATA_T, EDATA_T> extends ContextBase<OID_T, VID_T, VDATA_T, EDATA_T> {
    void Init(ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T> frag,
              ParallelMessageManager messageManager, StdVector<FFIByteString> args);

    void Output(ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T> frag);
}
