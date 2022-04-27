package com.alibaba.graphscope.graph;

import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graph.impl.VertexDataManagerImpl;
import java.util.List;
import org.apache.hadoop.io.Writable;

public interface VertexDataManager<VDATA_T> {
    void init(IFragment<Long,Long,VDATA_T,?> iFragment);

    VDATA_T getVertexData(long lid);

    void setVertexData(long lid, VDATA_T vertexData);

    <VDATA_T2> VertexDataManager<VDATA_T2> withNewVertexData(List<VDATA_T2> newVertexData);

    void setIFragment(IFragment<Long,Long,VDATA_T,?> fragment);

    void setValues(List<VDATA_T> vdatas);
}