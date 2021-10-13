/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
