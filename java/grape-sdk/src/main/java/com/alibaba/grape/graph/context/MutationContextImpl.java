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

import com.alibaba.fastffi.FFIVector;
import com.alibaba.grape.fragment.partitioner.HashcodePartitioner;
import com.alibaba.grape.fragment.partitioner.PartitionerBase;
import com.alibaba.grape.jobConf.JobConf;

import java.io.IOException;

public class MutationContextImpl<OID_T, VDATA_T, EDATA_T>
        implements MutationContext<OID_T, VDATA_T, EDATA_T> {
    private FFIVector<FFIVector<OID_T>> vidBuffers;
    private FFIVector<FFIVector<VDATA_T>> vdataBuffers;
    private FFIVector<FFIVector<OID_T>> esrcBuffers;
    private FFIVector<FFIVector<OID_T>> edstBuffers;
    private FFIVector<FFIVector<EDATA_T>> edataBuffers;

    private int workerNum;
    private int workerId;
    private JobConf jobConf;
    private PartitionerBase<OID_T> partitioner;

    public MutationContextImpl(JobConf jobConf, int workerId, int workerNum,
                               FFIVector<FFIVector<OID_T>> vidBuffers,
                               FFIVector<FFIVector<VDATA_T>> vdataBuffers,
                               FFIVector<FFIVector<OID_T>> esrcBuffers,
                               FFIVector<FFIVector<OID_T>> edstBuffers,
                               FFIVector<FFIVector<EDATA_T>> edataBuffers) {
        this.jobConf = jobConf;
        this.workerId = workerId;
        this.workerNum = workerNum;
        this.vidBuffers = vidBuffers;
        this.vdataBuffers = vdataBuffers;
        this.esrcBuffers = esrcBuffers;
        this.edstBuffers = edstBuffers;
        this.edataBuffers = edataBuffers;
        this.partitioner = (PartitionerBase<OID_T>) new HashcodePartitioner(workerNum);
    }

    @Override
    public JobConf getConfiguration() {
        return this.jobConf;
    }

    @Override
    public int getNumWorkers() {
        return this.workerNum;
    }

    @Override
    public void addVertexSimple(OID_T oid, VDATA_T vdata) {
        int fid = partitioner.GetPartitionId(oid);
        vidBuffers.get(fid).push_back(oid);
        vdataBuffers.get(fid).push_back(vdata);
    }

    @Override
    public void addEdgeRequest(OID_T sourceVertexId, OID_T dstVertexId, EDATA_T edata)
            throws IOException {
        int fid = partitioner.GetPartitionId(sourceVertexId);
        // int fid = workerId;
        esrcBuffers.get(fid).push_back(sourceVertexId);
        edstBuffers.get(fid).push_back(dstVertexId);
        edataBuffers.get(fid).push_back(edata);

        // push twice if dst ans src are not in the same fid.
        if (fid != partitioner.GetPartitionId(dstVertexId)) {
            fid = partitioner.GetPartitionId(dstVertexId);
            esrcBuffers.get(fid).push_back(sourceVertexId);
            edstBuffers.get(fid).push_back(dstVertexId);
            edataBuffers.get(fid).push_back(edata);
        }
    }
    // @Override
    // public StdVector<StdVector<EDATA_T>> getEdataBuffers() {
    //   return edataBuffers;
    // }
}