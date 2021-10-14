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

package io.graphscope.example.sssp;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.grape.ds.GSVertexArray;
import com.alibaba.grape.ds.VertexRange;
import com.alibaba.grape.ds.VertexSet;
import com.alibaba.grape.stdcxx.StdVector;
import io.graphscope.context.LabeledVertexDataContext;
import io.graphscope.fragment.ArrowFragment;
import io.graphscope.parallel.PropertyMessageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SSSPDefaultContext extends LabeledVertexDataContext<Long, Double> {
    public List<VertexSet> curModified;
    public List<VertexSet> nextModified;
    public long sourceOid;
    public StdVector<GSVertexArray<Double>> partialResults;
    public static Logger logger = LoggerFactory.getLogger(SSSPDefaultContext.class.getName());

    /**
     * @param fragment
     * @param messageManager
     * @param jsonObject
     *            contains the user-defined parameters in json manner
     */
    @Override
    public void init(ArrowFragment<Long> fragment, PropertyMessageManager messageManager, JSONObject jsonObject) {
        // must be called
        createFFIContext(fragment, Long.class, Double.class);
        logger.info("params size " + jsonObject.size() + ", " + jsonObject.toJSONString());
        int labelNum = fragment.vertexLabelNum();
        partialResults = data();
        logger.info("partial result " + partialResults.getAddress() + ",size " + partialResults.size());

        curModified = new ArrayList<>();
        nextModified = new ArrayList<>();
        for (int i = 0; i < labelNum; ++i) {
            VertexRange<Long> vertices = fragment.vertices(i);
            curModified.add(new VertexSet(vertices));
            nextModified.add(new VertexSet(vertices));
            logger.info("range " + partialResults.get(i).GetVertexRange().begin().GetValue() + ", "
                    + partialResults.get(i).GetVertexRange().end().GetValue());
            partialResults.get(i).SetValue(Double.MAX_VALUE);
        }
        sourceOid = jsonObject.getLong("src");
        if (Objects.isNull(sourceOid)) {
            logger.error("source Oid not set in parameter.");
            return;
        }
    }
}
