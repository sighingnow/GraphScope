package com.alibaba.graphscope.runtime;

import com.alibaba.graphscope.graphx.GrapeEdgePartition;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NativeUtils {

    private static Logger logger = LoggerFactory.getLogger(NativeUtils.class.getName());

    static{
        System.loadLibrary("grape-jni");
        logger.info("[NativeUtils:] load jni lib success");
    }
    public static native long createLoader();

    public static native long invokeLoadingAndProjection(long addr, int vdType, int edType);

    public static native long getArrowProjectedFragment(long fragId, String fragName);

    public static native long nativeCreateEdgePartition(String mmFiles, long size, int edType);

    public static <OID, VID, ED> GrapeEdgePartition<OID, VID, ED> createEdgePartition(
        String mmFiles, long size,
        Class<? extends OID> oidClass, Class<? extends VID> vidClass, Class<? extends ED> edClass)
        throws ClassNotFoundException, InvocationTargetException, InstantiationException, IllegalAccessException {
        String foreignName = "gs::EdgePartition<int64_t,uint64_t," + clz2Str(edClass) + ">";
        long addr = nativeCreateEdgePartition(mmFiles, size, clz2Int(edClass));
        if (addr <= 0) {
            throw new IllegalStateException("Fail to create edge partition");
        }
        return createEdgePartition(foreignName, addr);
    }

    private static GrapeEdgePartition createEdgePartition(String foreignName, long addr)
        throws ClassNotFoundException, InvocationTargetException, InstantiationException, IllegalAccessException {
        logger.info("[NativeUtils:] create edge partition for {}, addr {}", foreignName, addr);
        Class<? extends GrapeEdgePartition> clz = (Class<? extends GrapeEdgePartition>) FFITypeFactory.getType(
            GrapeEdgePartition.class, foreignName);
        Constructor[] constructors = clz.getConstructors();
        for (Constructor constructor : constructors) {
            if (constructor.getParameterCount() == 1
                && constructor.getParameterTypes()[0].getName().equals("long")) {
                return clz.cast(constructor.newInstance(addr));
            }
        }
        throw new IllegalStateException("No suitable constructor found");
    }

    private static String clz2Str(Class<?> clz) {
        if (clz.equals(Long.class) || clz.getName().equals("long")) {
            return "int64_t";
        } else if (clz.equals(Double.class) || clz.getName().equals("double")) {
            return "double";
        } else if (clz.equals(Integer.class) || clz.getName().equals("int")) {
            return "int32_t";
        } else {
            throw new IllegalStateException("Not supported ed class");
        }
    }

    private static int clz2Int(Class<?> clz) {
        if (clz.equals(Long.class) || clz.getName().equals("long")) {
            return 0;
        } else if (clz.equals(Double.class) || clz.getName().equals("double")) {
            return 1;
        } else if (clz.equals(Integer.class) || clz.getName().equals("int")) {
            return 2;
        } else {
            throw new IllegalStateException("Not supported ed class");
        }
    }
}
