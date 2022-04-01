package com.alibaba.graphscope.graph;

import com.alibaba.graphscope.fragment.IFragment;
import org.apache.hadoop.io.Writable;

public interface VertexDataManager<VDATA_T> {
    void init(IFragment<Long,Long,VDATA_T,?> iFragment);

    VDATA_T getVertexData(long lid);

    void setVertexData(long lid, VDATA_T vertexData);
}