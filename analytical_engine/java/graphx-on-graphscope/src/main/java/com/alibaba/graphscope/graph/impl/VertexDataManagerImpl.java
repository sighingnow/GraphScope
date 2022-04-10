package com.alibaba.graphscope.graph.impl;

import com.alibaba.graphscope.conf.GraphXConf;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graph.VertexDataManager;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VertexDataManagerImpl<VD> implements VertexDataManager<VD> {
    private static Logger logger = LoggerFactory.getLogger(VertexDataManagerImpl.class.getName());
    private GraphXConf conf;
    private Class<? extends VD> vDataClass;
    private VD[] values;

    public VertexDataManagerImpl(GraphXConf conf){
        this.conf = conf;
        vDataClass = conf.getVdataClass();
    }

    @Override
    public void init(IFragment<Long, Long, VD, ?> fragment) {
        values = (VD[]) new Object[Math.toIntExact((Long) fragment.getVerticesNum())];
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        for (long lid = 0; lid < values.length; ++lid){
            vertex.SetValue(lid);
            values[(int) lid] = fragment.getData(vertex);
        }
        logger.info("Create Vertex Data Manager: " + fragment.getVerticesNum());
    }

    @Override
    public VD getVertexData(long lid) {
        return values[(int)lid];
    }

    @Override
    public void setVertexData(long lid, VD vertexData) {
        values[(int) lid] = vertexData;
    }
}
