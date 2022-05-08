package com.alibaba.graphscope.utils;

import com.alibaba.graphscope.graphx.GrapeEdgePartition;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReflectUtils {
    private static Logger logger = LoggerFactory.getLogger(ReflectUtils.class.getName());

    private static String NATIVE_UTILS = "com.alibaba.graphscope.runtime.NativeUtils";
    private static Class<?> nativeUtilsClz;
    private static Method createEdgePartitionMethod;

    static {
        try {
            nativeUtilsClz = ReflectUtils.class.getClassLoader().loadClass(NATIVE_UTILS);
            if (nativeUtilsClz == null) {
                throw new IllegalStateException("Failed to load nativeUtils clz");
            }
            createEdgePartitionMethod = nativeUtilsClz.getDeclaredMethod("createEdgePartition", Class.class, Class.class, Class.class);
            if (createEdgePartitionMethod == null) {
                throw new IllegalStateException("method null");
            }
            logger.info("[ReflectUtils: ] got method {}", createEdgePartitionMethod.getName());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
    }

    public static <OID, VID, ED> GrapeEdgePartition<OID, VID, ED> invokeEdgePartitionCreation(
        Class<? extends OID> oidClass, Class<? extends VID> vdClass,
        Class<? extends ED> edClass)
        throws InvocationTargetException, IllegalAccessException {
        logger.info("invoke EdgePartition creation with params ed {}", edClass.getName());
        return (GrapeEdgePartition<OID, VID, ED>) createEdgePartitionMethod.invoke(null, oidClass, vdClass, edClass);
    }

}
