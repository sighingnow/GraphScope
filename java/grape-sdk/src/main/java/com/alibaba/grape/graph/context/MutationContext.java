/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.alibaba.grape.graph.context;

import com.alibaba.grape.jobConf.JobConf;

import java.io.IOException;

/**
 * MutationContext 定义了支持图拓扑变化的接口，包括增加/删除点或边.
 *
 * @param <OID_T>   Vertex ID 类型
 * @param <VDATA_T> Vertex Value 类型
 * @param <EDATA_T> Edge Value 类型
 */
@SuppressWarnings("rawtypes")
public interface MutationContext<OID_T, VDATA_T, EDATA_T> {
    public JobConf getConfiguration();

    public int getNumWorkers();

    public void addVertexSimple(OID_T oid, VDATA_T vdata);

    public void addEdgeRequest(OID_T sourceVertexId, OID_T dstVertexId, EDATA_T edge)
            throws IOException;

}
