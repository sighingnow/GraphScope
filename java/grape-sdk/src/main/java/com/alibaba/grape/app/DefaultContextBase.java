package com.alibaba.grape.app;

import com.alibaba.fastffi.FFIByteString;
import com.alibaba.grape.fragment.ImmutableEdgecutFragment;
import com.alibaba.grape.parallel.DefaultMessageManager;
import com.alibaba.grape.stdcxx.StdVector;

public interface DefaultContextBase<OID_T, VID_T, VDATA_T, EDATA_T> extends ContextBase<OID_T, VID_T, VDATA_T, EDATA_T> {
    void Init(ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T> frag, DefaultMessageManager messageManager, StdVector<FFIByteString> args);

    void Output(ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T> frag);
}