package com.alibaba.grape.ds;

import com.alibaba.ffi.FFITypeFactory;
import org.junit.Assert;
import org.junit.Test;
import org.scijava.nativelib.NativeLoader;

import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.grape.utils.CPP_LIBRARY_STRINGS.GRAPE_JNI_LIBRARY;

public class VertexRangeTest {
    static {
        try {
            NativeLoader.loadLibrary(GRAPE_JNI_LIBRARY);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private VertexRange.Factory factory = FFITypeFactory.getFactory(VertexRange.class, "grape::VertexRange<uint64_t>");

    @Test
    public void test1() {
        VertexRange<Long> vertices = factory.create();
        vertices.SetRange(0L, 101L);
        int expectedSum = 5050;
        int cnt = 0;
        for (Vertex<Long> vertex : vertices) {
            cnt += vertex.GetValue();
        }
        Assert.assertEquals(expectedSum, cnt);
    }

    @Test
    public void tes2() {
        VertexRange<Long> vertices = factory.create();
        vertices.SetRange(0L, 101L);
        int expectedSum = 5050;
        AtomicInteger cnt = new AtomicInteger(0);
        vertices.forEach((vertex) -> {
            cnt.getAndAdd(vertex.GetValue().intValue());
        });
        Assert.assertEquals(expectedSum, cnt.get());
    }
}
