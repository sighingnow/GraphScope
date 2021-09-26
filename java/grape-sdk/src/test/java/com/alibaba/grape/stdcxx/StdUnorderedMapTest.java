package com.alibaba.grape.stdcxx;

import com.alibaba.ffi.FFITypeFactory;
import com.alibaba.grape.utils.CPP_CLASSES_STRINGS;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.scijava.nativelib.NativeLoader;

import static com.alibaba.grape.utils.CPP_LIBRARY_STRINGS.GRAPE_JNI_LIBRARY;

public class StdUnorderedMapTest {
    static {
        try {
            NativeLoader.loadLibrary(GRAPE_JNI_LIBRARY);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private StdUnorderedMap.Factory<Integer, Long> factory = FFITypeFactory.getFactory(StdUnorderedMap.class,
            CPP_CLASSES_STRINGS.STD_UNORDERED_MAP + "<unsigned,uint64_t>");
    private StdUnorderedMap<Integer, Long> map;

    @Before
    public void init() {
        map = factory.create();
    }

    @Test
    public void test1() {
        for (int i = 0; i < 10; ++i) {
            map.set(i, (long) i);
        }
        for (int i = 0; i < 10; ++i) {
            Assert.assertTrue("Different at " + i, i == map.get(i).intValue());
        }
    }
}
