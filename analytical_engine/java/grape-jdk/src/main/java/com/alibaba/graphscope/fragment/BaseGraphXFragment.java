package com.alibaba.graphscope.fragment;

import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.graphscope.ds.PropertyNbrUnit;
import com.alibaba.graphscope.ds.Vertex;

public interface BaseGraphXFragment<OID_T, VID_T, VD_T, ED_T>
    extends EdgecutFragment<OID_T, VID_T, VD_T, ED_T>  {
    long id();

    @FFINameAlias("GetIEBegin")
    PropertyNbrUnit<VID_T> getIEBegin(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetIEEnd")
    PropertyNbrUnit<VID_T> getIEEnd(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetOEBegin")
    PropertyNbrUnit<VID_T> geOEBegin(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetOEEnd")
    PropertyNbrUnit<VID_T> getOEEnd(@CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetInEdgeNum")
    long getInEdgeNum();

    @FFINameAlias("GetOutEdgeNum")
    long getOutEdgeNum();
}
