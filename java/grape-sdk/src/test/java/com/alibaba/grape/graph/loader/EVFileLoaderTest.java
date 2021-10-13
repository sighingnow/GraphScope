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

package com.alibaba.grape.graph.loader;

import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.fastffi.FFIVector;
import com.alibaba.grape.app.lineparser.EVLineParserBase;
import com.alibaba.grape.graph.context.MutationContext;
import com.alibaba.grape.graph.loader.evfileLoader.EVFileLoader;
import com.alibaba.grape.jobConf.JobConf;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import org.junit.Before;
import org.junit.Test;

public class EVFileLoaderTest {

  private EVFileLoader<Long, Long, Double> loader;
  private FFIVector.Factory longFactory = FFITypeFactory.getFactory(
      FFIVector.class, "std::vector<std::vector<jlong>>");
  private FFIVector.Factory doubleFactory = FFITypeFactory.getFactory(
      FFIVector.class, "std::vector<std::vector<jdouble>>");
  private JobConf job;
  private int workerNum = 1;
  private int workerId = 0;
  public FFIVector<FFIVector<Long>> vidBuffers;
  public FFIVector<FFIVector<Long>> vdataBuffers;
  public FFIVector<FFIVector<Long>> esrcBuffers;
  public FFIVector<FFIVector<Long>> edstBuffers;
  public FFIVector<FFIVector<Double>> edataBuffers;

  public static class LineParser
      implements EVLineParserBase<Long, Long, Double> {

    @Override
    public void loadVertexLine(String fields,
                               MutationContext<Long, Long, Double> context)
        throws IOException {
      String[] strs = fields.split(" ");
      if (strs.length != 2)
        return;
      context.addVertexSimple(Long.valueOf(strs[0]), Long.valueOf(strs[1]));
    }

    @Override
    public void loadEdgeLine(String fields,
                             MutationContext<Long, Long, Double> context)
        throws IOException {
      String[] strs = fields.split(" ");
      if (strs.length != 3)
        return;
      context.addEdgeRequest(Long.valueOf(strs[0]), Long.valueOf(strs[1]),
                             Double.valueOf(strs[2]));
    }
  }

  @Before
  public void init() {
    job = new JobConf();
    try {
      URL vres = getClass().getClassLoader().getResource("p2p-31.v");
      job.setVfilePath(Paths.get(vres.toURI()).toFile().getAbsolutePath());
      URL eres = getClass().getClassLoader().getResource("p2p-31.e");
      job.setEfilePath(Paths.get(eres.toURI()).toFile().getAbsolutePath());
    } catch (Exception e) {
      e.printStackTrace();
    }
    job.setEVLineParserClassName(LineParser.class.getName());

    vidBuffers = longFactory.create();
    vidBuffers.resize(workerNum);
    vdataBuffers = longFactory.create();
    vdataBuffers.resize(workerNum);
    esrcBuffers = longFactory.create();
    esrcBuffers.resize(workerNum);
    edstBuffers = longFactory.create();
    edstBuffers.resize(workerNum);
    edataBuffers = doubleFactory.create();
    edataBuffers.resize(workerNum);
  }

  @Test
  public void test1() {
    loader = new EVFileLoader<>(workerId, workerNum, job);
    try {
      loader.loadFragment(vidBuffers, vdataBuffers, esrcBuffers, edstBuffers,
                          edataBuffers);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
