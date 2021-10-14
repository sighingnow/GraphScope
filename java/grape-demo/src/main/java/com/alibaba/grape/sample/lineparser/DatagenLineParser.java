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

import com.alibaba.grape.app.lineparser.*;
import com.alibaba.grape.graph.context.MutationContext;
import com.google.common.base.Splitter;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class DatagenLineParser implements EVLineParserBase<Long, Long, Double> {
    @Override
    public void loadEdgeLine(String data, MutationContext<Long, Long, Double> context) throws IOException {
        String[] splited = data.split("\\s+");
        // List<String> splited = Splitter.on('\t').omitEmptyStrings().splitToList(data);

        context.addEdgeRequest(Long.valueOf(splited[0]), Long.valueOf(splited[1]), Double.valueOf(splited[2]));
        if (!context.getConfiguration().getDirected()) {
            context.addEdgeRequest(Long.valueOf(splited[1]), Long.valueOf(splited[0]), Double.valueOf(splited[2]));
        }
    }

    @Override
    public void loadVertexLine(String data, MutationContext<Long, Long, Double> context) throws IOException {
        String[] splited = data.split("\\s+");
        // vadata equals 0 since datagen doesn't provide vadata
        // List<String> splited = Splitter.on('\t').omitEmptyStrings().splitToList(data);
        context.addVertexSimple(Long.valueOf(splited[0]), 0L);
    }
}
