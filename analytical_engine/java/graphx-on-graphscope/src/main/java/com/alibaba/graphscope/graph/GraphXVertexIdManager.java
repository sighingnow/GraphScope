package com.alibaba.graphscope.graph;

import com.alibaba.graphscope.fragment.IFragment;

/**
 * store vid, gid, oid mapping.
 */
public interface GraphXVertexIdManager extends VertexIdManager<Long,Long>{
    void init(IFragment<Long,Long,?,?> fragment);

    Long lid2Oid(Long lid);

    long innerVerticesNum();

    long verticesNum();
}
