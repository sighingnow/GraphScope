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

package io.graphscope.example.projected;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.grape.ds.GSVertexArray;
import com.alibaba.grape.ds.VertexRange;
import com.alibaba.grape.ds.VertexSet;
import com.alibaba.grape.fragment.ArrowProjectedFragment;
import com.alibaba.grape.parallel.DefaultMessageManager;
import io.graphscope.context.VertexDataContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSSPProjectedContext extends VertexDataContext<ArrowProjectedFragment<Long, Long, Double, Long>, Long> {
    public long sourceOid = -1;
    public GSVertexArray<Long> partialResults;
    public VertexSet curModified;
    public VertexSet nextModified;
    public static Logger logger = LoggerFactory.getLogger(SSSPProjectedContext.class.getName());

    @Override
    public void init(ArrowProjectedFragment<Long, Long, Double, Long> fragment, DefaultMessageManager messageManager,
            JSONObject jsonObject) {
        createFFIContext(fragment, Long.class, true);
        partialResults = data();
        VertexRange<Long> vertices = fragment.vertices();
        partialResults.Init(vertices, Long.MAX_VALUE);
        curModified = new VertexSet(vertices);
        nextModified = new VertexSet(vertices);

        sourceOid = jsonObject.getLong("src");
        if (!jsonObject.containsKey("src")) {
            logger.error("source Oid not set in parameter.");
            return;
        }
    }
}
