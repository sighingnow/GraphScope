package com.alibaba.grape.ds;

import com.alibaba.ffi.FFITypeFactory;
import org.junit.Assert;
import org.junit.Test;
import org.scijava.nativelib.NativeLoader;

import static com.alibaba.grape.utils.CPP_LIBRARY_STRINGS.GRAPE_JNI_LIBRARY;

public class NbrTest {
    static {
        try {
            NativeLoader.loadLibrary(GRAPE_JNI_LIBRARY);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Nbr.Factory<Long, Long> factory = FFITypeFactory.getFactory(Nbr.class, "grape::Nbr<uint64_t,int64_t>");

    @Test
    public void test1() {
        Nbr<Long, Long> nbr = factory.create(1L);
        Assert.assertEquals(1, nbr.neighbor().GetValue().longValue());
        Assert.assertEquals(1, nbr.neighbor().GetValue().longValue());
    }

    @Test
    public void test2() {
        Nbr<Long, Long> nbr = factory.craete(1L, 2L);
        Assert.assertEquals(2L, nbr.data().longValue());
    }

    @Test
    public void test3() {
        Nbr<Long, Long> nbr = factory.craete(1L, 2L);
        Nbr<Long, Long> nbr2 = nbr.copy();
        Assert.assertFalse(nbr.getAddress() == nbr2.getAddress());
        Assert.assertTrue(nbr.neighbor().GetValue().longValue() == nbr.neighbor().GetValue().longValue());
    }
}
