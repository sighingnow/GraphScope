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

package com.alibaba.grape.graph.loader.evfileLoader;

import com.alibaba.fastffi.FFIVector;
import com.alibaba.grape.app.lineparser.EVLineParserBase;
import com.alibaba.grape.graph.context.MutationContext;
import com.alibaba.grape.graph.context.MutationContextImpl;
import com.alibaba.grape.graph.loader.LoaderBase;
import com.alibaba.grape.jobConf.JobConf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.stream.Stream;

public class EVFileLoader<OID_T, VDATA_T, EDATA_T> implements LoaderBase<OID_T, VDATA_T, EDATA_T> {
    private int workerId;
    private int workerNum;
    private JobConf jobConf;
    private String efilePath;
    private String vfilePath;

    public EVFileLoader(int workerId, int workerNum, JobConf job) {
        this.jobConf = job;
        this.workerId = workerId;
        this.workerNum = workerNum;
        efilePath = jobConf.getEfilePath();
        vfilePath = jobConf.getVfilePath();
    }

    @Override
    public void loadFragment(FFIVector<FFIVector<OID_T>> vidBuffers,
                             FFIVector<FFIVector<VDATA_T>> vdataBuffers,
                             FFIVector<FFIVector<OID_T>> esrcBuffers,
                             FFIVector<FFIVector<OID_T>> edstBuffers,
                             FFIVector<FFIVector<EDATA_T>> edataBuffers) throws IOException {
        MutationContext<OID_T, VDATA_T, EDATA_T> mutationContext =
                new MutationContextImpl<OID_T, VDATA_T, EDATA_T>(jobConf, workerId, workerNum, vidBuffers,
                        vdataBuffers, esrcBuffers, edstBuffers,
                        edataBuffers);
        EVLineParserBase evLineParser = createEVLineParser(jobConf.getEVLineParser());

        //load vfile
        {
            BufferedReader reader = null;
            try {
                File file = new File(vfilePath);
                reader = new BufferedReader(new FileReader(file));
                Stream<String> lines = reader.lines();
                Iterator<String> lineIterator = lines.iterator();
                int i = 0;
                int totalLines = 0;
                for (; (i < workerId) && (lineIterator.hasNext()); ++i) {
                    lineIterator.next();
                    totalLines += 1;
                }
                int numLines = 0;
                int cnt;
                if (i == workerId) {
                    while (lineIterator.hasNext()) {
                        evLineParser.loadVertexLine(lineIterator.next(), mutationContext);
                        numLines += 1;
                        totalLines += 1;
                        cnt = 1;
                        while (lineIterator.hasNext() && cnt < workerNum) {
                            cnt += 1;
                            totalLines += 1;
                            lineIterator.next();
                        }
                    }
                }
                //if i!= workerId, it means there are less than workerNum lines
                reader.close();
                System.out.println("Finish processing vfile, worker " + workerId + " processed " + numLines
                        + "out of " + totalLines + " lines");
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                }
            }
        }


        //load efile
        {
            BufferedReader reader = null;
            try {
                File file = new File(efilePath);
                reader = new BufferedReader(new FileReader(file));
                Stream<String> lines = reader.lines();
                Iterator<String> lineIterator = lines.iterator();
                int i = 0;
                int totoalLines = 0;
                for (; (i < workerId) && (lineIterator.hasNext()); ++i) {
                    lineIterator.next();
                    totoalLines += 1;
                }
                int numLines = 0;
                int cnt;

                if (i == workerId) {
                    while (lineIterator.hasNext()) {
                        evLineParser.loadEdgeLine(lineIterator.next(), mutationContext);
                        numLines += 1;
                        totoalLines += 1;
                        cnt = 1;
                        while (lineIterator.hasNext() && cnt < workerNum) {
                            cnt += 1;
                            totoalLines += 1;
                            lineIterator.next();
                        }
                    }
                }
                //if i!= workerId, it means there are less than workerNum lines
                reader.close();
                System.out.println("Finish processing efile, worker " + workerId + " processed " + numLines
                        + "out of " + totoalLines + " lines");
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                }
            }
        }
    }


//      while ((tempString = reader.readLine()) != null) {
//        if (cnt % workerNum == workerId){
//          evLineParser.loadVertexLine(tempString, mutationContext);
//          lines += 1;
//        }
//        // mutationContext.addVertexSimple((OID_T) splited[0], (VDATA_T) splited[1]);
//        cnt += 1;
//      }


//    file = new File(efilePath);
//    try {
    // count number of lines


//      reader = new BufferedReader(new FileReader(file));
//      String tempString = null;
//      int cnt = 0;
//      int lines = 0;
//      while ((tempString = reader.readLine()) != null) {
//        if (cnt % workerNum == workerId){
//          evLineParser.loadEdgeLine(tempString, mutationContext);
//          lines += 1;
//        }
    // mutationContext.addEdgeRequest((OID_T) splited[0], (OID_T) splited[1],
    //                                (EDATA_T) splited[2]);
//        cnt += 1;
//      }
//      reader.close();

//    } catch (IOException e) { e.printStackTrace(); } finally {
//      if (reader != null) {
//        try {
//          reader.close();
//        } catch (IOException e1) { e1.printStackTrace(); }
//      }
//    }
//  }

    @SuppressWarnings("rawtypes")
    private EVLineParserBase createEVLineParser(Class<? extends EVLineParserBase> clz) {
        EVLineParserBase graphLoader = null;
        try {
            graphLoader = clz.newInstance();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return graphLoader;
    }
}
