package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISerializable;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.utils.CppClassName;
import com.alibaba.graphscope.utils.CppHeaderName;
import java.io.Serializable;

@FFIGen(library = "grape-jni")
@CXXHead(CppHeaderName.CORE_JAVA_GRAPHX_GRAPHX_VERTEX_MAP_H)
@CXXHead(CppHeaderName.CORE_JAVA_TYPE_ALIAS_H)
@FFITypeAlias(CppClassName.GS_GRAPHX_GRAPHX_VERTEX_MAP)
public interface GraphXVertexMap<OID_T,VID_T> extends FFISerializable, Serializable {
    long id();
    int fid();
    int fnum();

    @FFINameAlias("GetId")
    @CXXValue
    OID_T getId(VID_T vertex);

    @FFINameAlias("GetFragId")
    @CXXValue int getFragId(VID_T lid);

    @FFINameAlias("GetVertex")
    boolean getVertex(OID_T oid, @CXXReference Vertex<VID_T> vertex);

    @FFINameAlias("GetTotalVertexSize")
    long getTotalVertexSize();

    @FFINameAlias("GetVertexSize")
    VID_T getVertexSize();

    @FFINameAlias("GetInnerVertexSize")
    long getInnerVertexSize(int fid);

    @FFINameAlias("InnerVertexLid2Oid")
    OID_T innerVertexLid2Oid(VID_T lid);

    @FFINameAlias("OuterVertexLid2Oid")
    OID_T outerVertexLid2Oid(VID_T lid);

    @FFINameAlias("GetOuterVertexSize")
    long getOuterVertexSize();

    @FFINameAlias("InnerOid2Gid")
    VID_T innerOid2Gid(OID_T oid);

    @FFINameAlias("GetOuterVertexGid")
    VID_T getOuterVertexGid(VID_T lid);

    @FFINameAlias("Fid2GraphxPid")
    int fid2GraphxPid(int fid);

    @FFINameAlias("OuterVertexGid2Vertex")
    boolean outerVertexGid2Vertex(VID_T gid, @CXXReference Vertex<VID_T> vertex);

    default long innerVertexSize(){
        return getInnerVertexSize(fid());
    }
}
