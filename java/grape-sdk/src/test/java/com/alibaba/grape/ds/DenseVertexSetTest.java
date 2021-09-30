package com.alibaba.grape.ds;

import com.alibaba.grape.utils.FFITypeFactoryhelper;
import org.junit.Assert;
import org.junit.Test;


public class DenseVertexSetTest {

    private DenseVertexSet<Long> denseVertexSet;

    @Test
    public void test1() {
        denseVertexSet = FFITypeFactoryhelper.newDenseVertexSet();
        VertexRange<Long> vertices = FFITypeFactoryhelper.newVertexRangeLong();
        vertices.SetRange(0L, 100L);
        denseVertexSet.Init(vertices);

        denseVertexSet.Init(vertices);
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        vertex.SetValue(1L);
        Assert.assertFalse(denseVertexSet.Exist(vertex));
        denseVertexSet.Insert(vertex);
        Assert.assertTrue(denseVertexSet.Exist(vertex));

        denseVertexSet.Clear();
        Assert.assertFalse(denseVertexSet.Exist(vertex));

        for (int i = 0; i < 50; ++i) {
            vertex.SetValue((long) i);
            denseVertexSet.Insert(vertex);
        }
        Assert.assertTrue(denseVertexSet.PartialCount(0L, 100L) == 50);
        Assert.assertFalse(denseVertexSet.Empty());
        Assert.assertFalse(denseVertexSet.PartialEmpty(0L, 100L));
        Assert.assertTrue(denseVertexSet.PartialEmpty(51L, 100L));
    }
}
