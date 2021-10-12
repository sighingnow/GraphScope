package com.alibaba.grape.graph.loader;

import com.alibaba.fastffi.FFIVector;
import com.alibaba.grape.stdcxx.StdVector;
import java.io.IOException;

/**
 * LoadBase是TunnelLoader, EVFileLoader等类的接口，声明类一个虚的loadFragment函数，
 * @param <OID_T>
 * @param <VDATA_T>
 * @param <EDATA_T>
 */
public interface LoaderBase<OID_T, VDATA_T, EDATA_T> {
  public abstract void loadFragment(FFIVector<FFIVector<OID_T>> vidBuffers,
                                    FFIVector<FFIVector<VDATA_T>> vdataBuffers,
                                    FFIVector<FFIVector<OID_T>> esrcBuffers,
                                    FFIVector<FFIVector<OID_T>> edstBuffers,
                                    FFIVector<FFIVector<EDATA_T>> edataBuffers) throws IOException;
}