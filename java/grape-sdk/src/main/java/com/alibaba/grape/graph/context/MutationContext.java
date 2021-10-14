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

package com.alibaba.grape.graph.context;

import com.alibaba.grape.jobConf.JobConf;

import java.io.IOException;

@SuppressWarnings("rawtypes")
public interface MutationContext<OID_T, VDATA_T, EDATA_T> {
    public JobConf getConfiguration();

    public int getNumWorkers();

    public void addVertexSimple(OID_T oid, VDATA_T vdata);

    public void addEdgeRequest(OID_T sourceVertexId, OID_T dstVertexId, EDATA_T edge) throws IOException;

}
