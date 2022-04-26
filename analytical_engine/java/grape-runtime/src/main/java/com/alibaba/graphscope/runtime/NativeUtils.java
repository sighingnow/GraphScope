package com.alibaba.graphscope.runtime;

public class NativeUtils {
    public static native long createLoader();
    public static native long invokeLoadingAndProjection(long addr, int vdType, int edType);

    public static native long getArrowProjectedFragment(long fragId,String fragName);
}
