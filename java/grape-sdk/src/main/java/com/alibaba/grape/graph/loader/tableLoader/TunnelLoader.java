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

package com.alibaba.grape.graph.loader.tableLoader;

import com.alibaba.fastffi.FFIVector;
import com.alibaba.grape.app.lineparser.*;
import com.alibaba.grape.graph.context.MutationContext;
import com.alibaba.grape.graph.context.MutationContextImpl;
// import com.aliyun.odps.data.TableInfo;
import com.alibaba.grape.graph.loader.*;
import com.alibaba.grape.graph.loader.tableLoader.converter.*;
import com.alibaba.grape.jobConf.JOB_CONF;
import com.alibaba.grape.jobConf.JobConf;
import com.alibaba.grape.stdcxx.StdVector;
import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.cupid.CupidConf;
import com.aliyun.odps.cupid.CupidSession;
import com.aliyun.odps.cupid.table.v1.tunnel.impl.Util;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

public class TunnelLoader<OID_T, VDATA_T, EDATA_T> implements LoaderBase<OID_T, VDATA_T, EDATA_T> {
  private Odps odps;
  private int workerId;
  private int workerNum;

  private String project;
  private ArrayList<TableInfo> tables;
  private JobConf graphJob;

  public TunnelLoader(int workerId, int workerNum, JobConf job) {
    this.workerId = workerId;
    this.workerNum = workerNum;
    graphJob = job;
    Map<String, String> env = System.getenv();
    String accessId = env.get("odps_id");
    if (accessId == null) {
      throw new RuntimeException("odps_id is not set as env variable.");
    }
    String accessKey = env.get("odps_key");
    if (accessKey == null) {
      throw new RuntimeException("odps_key is not set as env variable.");
    }
    String endPoint = env.get("odps_endpoint");
    if (endPoint == null) {
      throw new RuntimeException("odps_endpoint is not set as env "
                                 + "variable.");
    }

    tables = new ArrayList<TableInfo>();
    Gson gson = new GsonBuilder().disableHtmlEscaping().create();
    // System.out.println(graphJob.size());
    // System.out.println(graphJob.get(JOB_CONF.INPUT_TABLES));

    JsonArray inputInfo = gson.fromJson(graphJob.get(JOB_CONF.INPUT_TABLES), JsonArray.class);
    for (int i = 0; i < inputInfo.size(); i++) {
      JsonObject tableInfo = inputInfo.get(i).getAsJsonObject();
      project = tableInfo.get("projName").getAsString();
      TableInfo table = TableInfo.builder()
                            .projectName(project)
                            .tableName(tableInfo.get("tblName").getAsString())
                            .loaderClassName(tableInfo.get("loaderClassName").getAsString())
                            .build();
      JsonArray partSpec = tableInfo.get("partSpec").getAsJsonArray();
      LinkedHashMap<String, String> partition = new LinkedHashMap<String, String>();
      for (int j = 0; j < partSpec.size(); j++) {
        String part = partSpec.get(j).getAsJsonPrimitive().getAsString();
        String[] ss = part.split("=");
        if (ss.length != 2) {
          throw new RuntimeException("ODPS-0730001: error part spec format: " + part);
        }
        partition.put(ss[0], ss[1]);
      }
      table.setPartSpec(partition);
      tables.add(table);
    }

    if (project == null || "".equals(project)) {
      //   Map<String, String> env = System.getenv();
      project = env.get("grape_user_project");
    }

    CupidConf conf = new CupidConf();
    conf.set("odps.project.name", project);
    conf.set("odps.end.point", endPoint);
    conf.set("odps.access.id", accessId);
    conf.set("odps.access.key", accessKey);
    CupidSession.setConf(conf);

    this.odps = CupidSession.get().odps();
  }
  @Override
  public void loadFragment(FFIVector<FFIVector<OID_T>> vidBuffers,
                           FFIVector<FFIVector<VDATA_T>> vdataBuffers,
                           FFIVector<FFIVector<OID_T>> esrcBuffers,
                           FFIVector<FFIVector<OID_T>> edstBuffers,
                           FFIVector<FFIVector<EDATA_T>> edataBuffers) throws IOException {
    MutationContext mutationContext = new MutationContextImpl<OID_T, VDATA_T, EDATA_T>(
        graphJob, workerId, workerNum, vidBuffers, vdataBuffers, esrcBuffers, edstBuffers,
        edataBuffers);
    TableTunnel tunnel = new TableTunnel(odps);
    for (TableInfo table : tables) {
      Class<? extends RecordLineParser> loaderClass = table.getLoaderClass();
      RecordLineParser graphLoader = createGraphLoader(loaderClass);

      // create download session
      TableTunnel.DownloadSession session;
      LinkedHashMap<String, String> partition = table.getPartSpec();
      try {
        if (partition.isEmpty()) {
          session = tunnel.createDownloadSession(project, table.getTableName());
        } else {
          PartitionSpec odpsPartitionSpec = Util.toOdpsPartitionSpec(partition);
          session = tunnel.createDownloadSession(project, table.getTableName(), odpsPartitionSpec);
        }
      } catch (TunnelException e) { throw new IOException(e); }

      long recordCount = session.getRecordCount();
      // recordCount = 300;
      long numRecordPerSplit = (recordCount + workerNum - 1) / workerNum;
      long begin = Long.min(workerId * numRecordPerSplit, recordCount);
      long end = Long.min(begin + numRecordPerSplit, recordCount);
      long count = end - begin;

      TunnelRecordReader reader;
      try {
        reader = session.openRecordReader(begin, count, true);
      } catch (TunnelException e) { throw new IOException(e); }

      ArrayRecord record = new ArrayRecord(odps.tables().get(table.getTableName()).getSchema());
      SQLRecord rec = new SQLRecord(record.getColumns());
      int columnNum = record.getColumnCount();
      ColumnConverterBase[] parsers = new ColumnConverterBase[columnNum];
      Column[] columns = record.getColumns();
      for (int i = 0; i < columnNum; ++i) {
        if (columns[i].getType() == OdpsType.STRING) {
          parsers[i] = new StringConverter();
        } else if (columns[i].getType() == OdpsType.BIGINT) {
          parsers[i] = new BigIntConverter();
        } else if (columns[i].getType() == OdpsType.DOUBLE) {
          parsers[i] = new DoubleConverter();
        }
      }
      // for (long k = 0; k < Math.min(count, 300); ++k) {
      for (long k = 0; k < count; ++k) {
        record = (ArrayRecord) reader.read(record);
        for (int i = 0; i < columnNum; ++i) {
          parsers[i].arrayRecordToSQLRecord(i, record, rec);
        }
        graphLoader.load(k, rec, mutationContext);
      }
    }
  }
  @SuppressWarnings("rawtypes")
  private RecordLineParser createGraphLoader(Class<? extends RecordLineParser> clz) {
    RecordLineParser graphLoader = null;
    try {
      graphLoader = clz.newInstance();
    } catch (Exception e) { e.printStackTrace(); }
    return graphLoader;
  }
}