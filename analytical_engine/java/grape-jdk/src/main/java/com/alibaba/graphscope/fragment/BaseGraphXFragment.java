package com.alibaba.graphscope.fragment;

import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.graphscope.ds.PropertyNbrUnit;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.graphx.EdgeData;
import com.alibaba.graphscope.graphx.GraphXCSR;
import com.alibaba.graphscope.graphx.GraphXVertexMap;
import com.alibaba.graphscope.graphx.VertexData;

public interface BaseGraphXFragment<OID_T, VID_T, VD_T, ED_T>
    extends EdgecutFragment<OID_T, VID_T, VD_T, ED_T>  {
    long id();

    @FFINameAlias("GetIEBegin")
    PropertyNbrUnit<VID_T> getIEBegin(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetIEEnd")
    PropertyNbrUnit<VID_T> getIEEnd(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetOEBegin")
    PropertyNbrUnit<VID_T> getOEBegin(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetOEEnd")
    PropertyNbrUnit<VID_T> getOEEnd(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetCSR")
    @CXXReference GraphXCSR<VID_T> getCSR();

    @FFINameAlias("GetVM")
    @CXXReference GraphXVertexMap<OID_T,VID_T> getVM();

    @FFINameAlias("GetVdata")
    @CXXReference VertexData<VID_T,VD_T> getVdata();

    @FFINameAlias("GetEdata")
    @CXXReference EdgeData<VID_T,ED_T> getEdata();

    @FFINameAlias("GetInEdgeNum")
    long getInEdgeNum();

    @FFINameAlias("GetOutEdgeNum")
    long getOutEdgeNum();
}
