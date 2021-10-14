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

package io.graphscope.example.wcc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.grape.ds.GSVertexArray;
import com.alibaba.grape.ds.VertexRange;
import com.alibaba.grape.ds.VertexSet;
import com.alibaba.grape.stdcxx.StdVector;
import io.graphscope.example.sssp.SSSPDefaultContext;
import io.graphscope.context.LabeledVertexDataContext;
import io.graphscope.fragment.ArrowFragment;
import io.graphscope.parallel.PropertyMessageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class WCCDefaultContext extends LabeledVertexDataContext<Long, Long> {
    public StdVector<GSVertexArray<Long>> compId;
    public List<VertexSet> curModified;
    public List<VertexSet> nextModified;
    public static Logger logger = LoggerFactory.getLogger(SSSPDefaultContext.class.getName());

    @Override
    public void init(ArrowFragment<Long> fragment, PropertyMessageManager messageManager, JSONObject jsonObject) {
        createFFIContext(fragment, Long.class, Long.class);
        int labelNum = fragment.vertexLabelNum();
        curModified = new ArrayList<>();
        nextModified = new ArrayList<>();
        for (int i = 0; i < labelNum; ++i) {
            VertexRange<Long> vertices = fragment.vertices(i);
            curModified.add(new VertexSet(vertices));
            nextModified.add(new VertexSet(vertices));
            logger.info("range " + compId.get(i).GetVertexRange().begin().GetValue() + ", "
                    + compId.get(i).GetVertexRange().end().GetValue());
            compId.get(i).SetValue(0L);
        }
    }
}
