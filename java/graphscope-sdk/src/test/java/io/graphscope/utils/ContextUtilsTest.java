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

package io.graphscope.utils;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.grape.fragment.ArrowProjectedFragment;
import com.alibaba.grape.parallel.DefaultMessageManager;
import io.graphscope.context.LabeledVertexDataContext;
import io.graphscope.context.VertexDataContext;
import io.graphscope.fragment.ArrowFragment;
import io.graphscope.parallel.PropertyMessageManager;
import org.junit.Assert;
import org.junit.Test;

public class ContextUtilsTest {

    public static class SampleContext extends LabeledVertexDataContext<Long, Double> {
        @Override
        public void init(ArrowFragment<Long> fragment, PropertyMessageManager messageManager, JSONObject jsonObject) {
        }
    }

    public static class SampleContext2 extends VertexDataContext<ArrowProjectedFragment<Long, Long, Long, Double>, Double> {
        @Override
        public void init(ArrowProjectedFragment<Long, Long, Long, Double> fragment, DefaultMessageManager messageManager, JSONObject jsonObject) {

        }
    }

    @Test
    public void test() {
        Assert.assertTrue(ContextUtils.getPropertyCtxObjBaseClzName(new SampleContext()).equals("LabeledVertexDataContext"));
        Assert.assertTrue(ContextUtils.getProjectedCtxObjBaseClzName(new SampleContext2()).equals("VertexDataContext"));
    }
}
