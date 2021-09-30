package com.alibaba.grape.ds;

import com.alibaba.grape.utils.FFITypeFactoryhelper;
import org.junit.Assert;
import org.junit.Test;

public class VertexArrayTest {

    @Test
    public void test1() {
        VertexRange<Long> vertices = FFITypeFactoryhelper.newVertexRangeLong();
        vertices.SetRange(0L, 100L);
        VertexArray<Long, Long> vertexArray = FFITypeFactoryhelper.newVertexArray(Long.class);
        vertexArray.Init(vertices);
        for (Vertex<Long> vertex : vertices) {
            vertexArray.set(vertex, 1L);
        }
        for (Vertex<Long> vertex : vertices) {
            Assert.assertTrue(vertexArray.get(vertex).equals(1L));
        }
    }

    @Test
    public void test2() {
        VertexRange<Long> vertices = FFITypeFactoryhelper.newVertexRangeLong();
        vertices.SetRange(0L, 100L);
        VertexArray<Long, Long> vertexArray = FFITypeFactoryhelper.newVertexArray(Long.class);
        vertexArray.Init(vertices, 1L);
        for (Vertex<Long> vertex : vertices) {
            Assert.assertTrue(vertexArray.get(vertex).equals(1L));
        }
        vertexArray.SetValue(vertices, 2L);
        for (Vertex<Long> vertex : vertices) {
            Assert.assertTrue(vertexArray.get(vertex).equals(2L));
        }
    }
}
