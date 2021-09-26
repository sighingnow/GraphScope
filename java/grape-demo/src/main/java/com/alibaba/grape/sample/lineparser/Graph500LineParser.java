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
