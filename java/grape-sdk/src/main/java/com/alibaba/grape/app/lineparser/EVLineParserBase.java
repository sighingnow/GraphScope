package com.alibaba.grape.app.lineparser;

import com.alibaba.grape.graph.context.MutationContext;
import java.io.IOException;

public interface EVLineParserBase<OID_T, VDATA_T, EDATA_T> {
  void loadVertexLine(String fields, MutationContext<OID_T, VDATA_T, EDATA_T> context)
      throws IOException;
  void loadEdgeLine(String fields, MutationContext<OID_T, VDATA_T, EDATA_T> context)
      throws IOException;
}