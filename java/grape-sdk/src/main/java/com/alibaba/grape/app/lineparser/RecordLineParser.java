package com.alibaba.grape.app.lineparser;

import com.alibaba.grape.graph.context.MutationContext;
import com.aliyun.odps.io.WritableRecord;
import java.io.IOException;

/**
 * Lineparser interface for odps records
 * @param <VID_T>
 * @param <VDATA_T>
 * @param <EDATA_T>
 */
public interface RecordLineParser<VID_T, VDATA_T, EDATA_T> {
  void load(Long recordNum, WritableRecord record, MutationContext<VID_T, VDATA_T, EDATA_T> context)
      throws IOException;
}
