package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FragmentRDD {

    private static Logger logger = LoggerFactory.getLogger(FragmentRDD.class.getName());
//    private static BufferedWriter writer;

    private static String NATIVE_UTILS = "com.alibaba.graphscope.runtime.NativeUtils";
    private static Class<?> nativeClz;
    private static Method method;

    static {
        try {
            nativeClz = FragmentRDD.class.getClassLoader().loadClass(NATIVE_UTILS);
            if (nativeClz == null) {
                throw new IllegalStateException("failed to load nativeUtils clz");
            }
            method = nativeClz.getDeclaredMethod("getArrowProjectedFragment", long.class,
                String.class);
            if (method == null) {
                throw new IllegalStateException("method null");
            }
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            e.printStackTrace();
        }
    }

    private long address;
    private ArrowProjectedFragment projectedFragment;

    public FragmentRDD(long fragId, String foreignFragName, int numPartitions) throws IOException {
        try {
            address = (long) method.invoke(null, fragId, foreignFragName);
            if (address <= 0) {
                throw new IllegalStateException("Got an address less than zero");
            }
            projectedFragment = createArrowProjectedFragmentInstance(
                (Class<? extends ArrowProjectedFragment>) FFITypeFactory.getType(ArrowProjectedFragment.class, foreignFragName), address);
            logger.info("construct projected fragment: " + projectedFragment);
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException | InvocationTargetException e) {
            e.printStackTrace();
        }
        logger.info("Retried fragment: address" + address);
    }

    /**
     * factory method.
     *
     * @param fragId        fragment id.
     * @param numPartitions num partitions on this worker.
     * @return created fragment
     */
    public static synchronized FragmentRDD create(String fragId, String foreignFragName,
        int numPartitions) throws IOException {
        logger.info("creating fragment: " + fragId + ", " + numPartitions);
        return new FragmentRDD(Long.parseLong(fragId), foreignFragName, numPartitions);
    }

    private static ArrowProjectedFragment createArrowProjectedFragmentInstance(
        Class<? extends ArrowProjectedFragment> clz, long addr)
        throws InvocationTargetException, InstantiationException, IllegalAccessException {
        Constructor[] constructors = clz.getConstructors();
        for (Constructor constructor : constructors) {
            if (constructor.getParameterCount() == 1
                && constructor.getParameterTypes()[0].getName().equals("long")) {
                return clz.cast(constructor.newInstance(addr));
            }
        }
        throw new IllegalAccessException("No constructors found");
    }

}
