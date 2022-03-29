package com.alibaba.graphscope.graph;

/**
 * store vid, gid, oid mapping.
 */
public interface IdManager{

    long lid2Oid(long lid);

    long gid2Oid(long lid);

    long oid2Lid(long oid);

    long oid2Gid(long oid);

    long[] getOidArray();

    long innerVerticesNum();

    long verticesNum();
}
