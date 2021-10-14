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

import com.alibaba.grape.ds.VertexRange;
import io.graphscope.app.PropertyDefaultAppBase;
import io.graphscope.context.PropertyDefaultContextBase;
import io.graphscope.fragment.ArrowFragment;
import io.graphscope.parallel.PropertyMessageManager;

/**
 * @author xiaolei.zl
 * 
 * @date 2021/06/14
 */
public class Traverse implements PropertyDefaultAppBase<Long, TraverseDefaultContextBase> {
    @Override
    public void PEval(ArrowFragment<Long> fragment, PropertyDefaultContextBase<Long> context,
            PropertyMessageManager messageManager) {
        TraverseDefaultContextBase ctx = (TraverseDefaultContextBase) context;
        VertexRange<Long> innerVertices = fragment.innerVertices(0);
    }

    @Override
    public void IncEval(ArrowFragment<Long> fragment, PropertyDefaultContextBase<Long> context,
            PropertyMessageManager messageManager) {
        TraverseDefaultContextBase ctx = (TraverseDefaultContextBase) context;
        if (ctx.step >= ctx.maxStep) {
            return;
        }
        messageManager.ForceContinue();
    }
}
