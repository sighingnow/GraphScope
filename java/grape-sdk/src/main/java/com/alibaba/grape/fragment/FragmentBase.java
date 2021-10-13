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

package com.alibaba.grape.fragment;

import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.grape.ds.AdjList;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.ds.VertexRange;

public interface FragmentBase<OID_T, VID_T, VDATA_T, EDATA_T> extends FFIPointer {
    int fid();

    int fnum();

    @FFINameAlias("GetEdgeNum")
    long getEdgeNum();

    @FFINameAlias("GetVerticesNum")
    VID_T getVerticesNum();

    @FFINameAlias("GetTotalVerticesNum")
    long getTotalVerticesNum();

    @FFINameAlias("Vertices")
    @CXXValue VertexRange<VID_T> vertices();

    @FFINameAlias("GetVertex")
    boolean getVertex(@CXXReference OID_T oid, @CXXReference Vertex<VID_T> vertex);

    /**
     * Get the original Id
     *
     * @param vertex
     * @return OID
     */
    @FFINameAlias("GetId")
    @CXXValue OID_T getId(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetFragId")
    int getFragId(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetData")
    @CXXReference VDATA_T getData(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("SetData")
    void setData(@CXXReference Vertex<VID_T> vertex, @CXXReference VDATA_T val);

    @FFINameAlias("HasChild")
    boolean hasChild(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("HasParent")
    boolean hasParent(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetLocalInDegree")
    int getLocalInDegree(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetLocalOutDegree")
    int getLocalOutDegree(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("Gid2Vertex")
    boolean gid2Vertex(@CXXReference VID_T gid, @CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("Vertex2Gid")
    @CXXValue VID_T vertex2Gid(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetIncomingAdjList")
    @CXXValue AdjList<VID_T, EDATA_T> getIncomingAdjList(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetOutgoingAdjList")
    @CXXValue AdjList<VID_T, EDATA_T> getOutgoingAdjList(@CXXReference Vertex<VID_T> vertex);
}
