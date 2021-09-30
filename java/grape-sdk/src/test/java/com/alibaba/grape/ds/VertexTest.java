package com.alibaba.grape.ds;

import com.alibaba.grape.utils.FFITypeFactoryhelper;
import org.junit.Assert;
import org.junit.Test;
public class VertexTest {

    @Test
    public void test1() {
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        vertex.SetValue(1L);
        Assert.assertTrue(vertex.GetValue().equals(1L));
        Vertex<Long> other = FFITypeFactoryhelper.newVertexLong();
        other.SetValue(1L);
        Assert.assertTrue(vertex.eq(other));
    }

    @Test
    public void test2() {
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        vertex.SetValue(0L);
        int cnt = 0;
        while (vertex.GetValue().longValue() != 10) {
            vertex.inc();
            cnt += 1;
        }
        Assert.assertTrue(cnt == 10);
    }
}
