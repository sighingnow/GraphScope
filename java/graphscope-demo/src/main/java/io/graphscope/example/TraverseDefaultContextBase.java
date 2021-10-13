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

package io.graphscope.example;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.grape.ds.EmptyType;
import io.graphscope.context.LabeledVertexDataContext;
import io.graphscope.fragment.ArrowFragment;
import io.graphscope.parallel.PropertyMessageManager;

public class TraverseDefaultContextBase extends LabeledVertexDataContext<Long, EmptyType> {
    public int step;
    public int maxStep;

    public TraverseDefaultContextBase() {
        maxStep = 0;
        step = 0;
    }

    @Override
    public void init(ArrowFragment<Long> fragment, PropertyMessageManager messageManager, JSONObject jsonObject) {
        System.out.println("Enter java context init, args");
        System.out.println(jsonObject.toJSONString());
    }

}
