package com.alibaba.graphscope.utils;

import com.alibaba.graphscope.graphx.GrapeEdgePartition;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ReflectUtils {

    private static String NATIVE_UTILS = "com.alibaba.graphscope.runtime.NativeUtils";
    private static Class<?> nativeUtilsClz;
    private static Method createEdgePartitionMethod;

    static {
        try {
            nativeUtilsClz = ReflectUtils.class.getClassLoader().loadClass(NATIVE_UTILS);
            if (nativeUtilsClz == null) {
                throw new IllegalStateException("Failed to load nativeUtils clz");
            }
            createEdgePartitionMethod = nativeUtilsClz.getDeclaredMethod("createEdgePartition",
                String.class,
                long.class, Class.class, Class.class, Class.class);
            if (createEdgePartitionMethod == null) {
                throw new IllegalStateException("method null");
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
    }

    public static <OID, VID, ED> GrapeEdgePartition<OID, VID, ED> invokeEdgePartitionCreation(
        Class<? extends OID> oidClass, Class<? extends VID> vdClass,
        Class<? extends ED> edClass, String mmFiles, long size)
        throws InvocationTargetException, IllegalAccessException {
        return (GrapeEdgePartition<OID, VID, ED>) createEdgePartitionMethod.invoke(null, mmFiles,
            size, oidClass, vdClass, edClass);
    }

}
