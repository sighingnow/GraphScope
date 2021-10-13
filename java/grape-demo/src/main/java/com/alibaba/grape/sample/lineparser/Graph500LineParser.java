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

package com.alibaba.grape.sample.lineparser;

import com.alibaba.grape.app.lineparser.EVLineParserBase;
import com.alibaba.grape.graph.context.MutationContext;

import java.io.IOException;

public class Graph500LineParser implements EVLineParserBase<Long, Long,Double> {
    @Override
    public void loadEdgeLine(String data, MutationContext<Long, Long, Double> context)
            throws IOException {
        String[] fields = data.split("\\s+");
        context.addEdgeRequest(Long.valueOf(fields[0]), Long.valueOf(fields[1]),
                0.0);
    }

    @Override
    public void loadVertexLine(String data, MutationContext<Long, Long, Double> context)
            throws IOException {
        String[] fields = data.split("\\s+");
        // vadata equals 0 since datagen doesn't provide vadata
        context.addVertexSimple(Long.valueOf(fields[0]), 0L);
    }
}
