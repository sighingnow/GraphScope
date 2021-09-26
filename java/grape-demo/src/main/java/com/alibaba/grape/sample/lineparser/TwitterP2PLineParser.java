package com.alibaba.grape.sample.lineparser;

import com.alibaba.grape.app.lineparser.*;
import com.alibaba.grape.graph.context.MutationContext;
import java.io.IOException;

public class TwitterP2PLineParser implements EVLineParserBase<Long, Long, Double> {
  @Override
  public void loadEdgeLine(String data, MutationContext<Long, Long, Double> context)
      throws IOException {
    String[] fields = data.split("\\s+");
    context.addEdgeRequest(Long.valueOf(fields[0]), Long.valueOf(fields[1]),
                           Double.valueOf(fields[2]));
  }

  @Override
  public void loadVertexLine(String data, MutationContext<Long, Long, Double> context)
      throws IOException {
    String[] fields = data.split("\\s+");
    context.addVertexSimple(Long.valueOf(fields[0]), Long.valueOf(fields[1]));
  }
}