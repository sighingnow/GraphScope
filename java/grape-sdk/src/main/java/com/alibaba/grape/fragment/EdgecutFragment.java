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
import com.alibaba.grape.ds.AdjList;
import com.alibaba.grape.ds.DestList;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.ds.VertexRange;

public interface EdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T>
        extends FragmentBase<OID_T, VID_T, VDATA_T, EDATA_T> {
    @FFINameAlias("GetInnerVerticesNum")
    @CXXValue VID_T getInnerVerticesNum();

    @FFINameAlias("GetOuterVerticesNum")
    @CXXValue VID_T getOuterVerticesNum();

    @FFINameAlias("InnerVertices")
    @CXXValue VertexRange<VID_T> innerVertices();

    @FFINameAlias("OuterVertices")
    @CXXValue VertexRange<VID_T> outerVertices();

    @FFINameAlias("IsInnerVertex")
    boolean isInnerVertex(@CXXReference Vertex<VID_T> v);

    @FFINameAlias("IsOuterVertex")
    boolean isOuterVertex(@CXXReference Vertex<VID_T> v);

    @FFINameAlias("GetInnerVertex")
    boolean getInnerVertex(@CXXReference OID_T oid, @CXXReference Vertex<VID_T> v);

    @FFINameAlias("GetOuterVertex")
    boolean getOuterVertex(@CXXReference OID_T oid, @CXXReference Vertex<VID_T> v);

    @FFINameAlias("GetInnerVertexId")
    @CXXValue OID_T getInnerVertexId(@CXXReference Vertex<VID_T> v);

    @FFINameAlias("GetOuterVertexId")
    @CXXValue OID_T getOuterVertexId(@CXXReference Vertex<VID_T> v);

    @FFINameAlias("InnerVertexGid2Vertex")
    boolean innerVertexGid2Vertex(@CXXReference VID_T gid, @CXXReference Vertex<VID_T> v);

    @FFINameAlias("OuterVertexGid2Vertex")
    boolean outerVertexGid2Vertex(@CXXReference VID_T gid, @CXXReference Vertex<VID_T> v);

    @FFINameAlias("GetOuterVertexGid")
    @CXXValue VID_T getOuterVertexGid(@CXXReference Vertex<VID_T> v);

    @FFINameAlias("GetInnerVertexGid")
    @CXXValue VID_T getInnerVertexGid(@CXXReference Vertex<VID_T> v);

    @FFINameAlias("IEDests")
    @CXXValue DestList inEdgeDests(@CXXReference Vertex<VID_T> v);

    @FFINameAlias("OEDests")
    @CXXValue DestList outEdgeDests(@CXXReference Vertex<VID_T> v);

    @FFINameAlias("IOEDests")
    @CXXValue DestList inOutEdgeDests(@CXXReference Vertex<VID_T> v);

    @FFINameAlias("GetIncomingInnerVertexAdjList")
    @CXXValue AdjList<VID_T, EDATA_T> getIncomingInnerVertexAdjList(@CXXReference Vertex<VID_T> v);

    @FFINameAlias("GetOutgoingInnerVertexAdjList")
    @CXXValue AdjList<VID_T, EDATA_T> getOutgoingInnerVertexAdjList(@CXXReference Vertex<VID_T> v);
}
