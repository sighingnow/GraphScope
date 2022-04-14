package com.alibaba.graphscope.graph.impl;

import com.alibaba.graphscope.conf.GraphXConf;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graph.VertexDataManager;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DoubleVertexDataManagerImpl implements VertexDataManager<Double> {
    private static Logger logger = LoggerFactory.getLogger(DoubleVertexDataManagerImpl.class.getName());
    private GraphXConf conf;
    private double[] values;

    public DoubleVertexDataManagerImpl(GraphXConf conf){
        this.conf = conf;
        if (!conf.getVdataClass().equals(Double.class)){
            throw new IllegalStateException("Type inconsistent: " + conf.getVdataClass());
        }
    }

    @Override
    public void init(IFragment<Long, Long, Double, ?> fragment) {
        values = new double[Math.toIntExact((Long) fragment.getVerticesNum())];
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        long innerVerticesNum = fragment.getInnerVerticesNum();
        for (long lid = 0; lid < innerVerticesNum; ++lid){
            vertex.SetValue(lid);
            values[(int) lid] = fragment.getData(vertex);
//            logger.info("vdata lid [{}] value [{}]", lid, values[(int) lid]);
        }
        for (long lid = innerVerticesNum; lid < values.length; ++lid) {
            values[(int) lid] = (Double) Double.POSITIVE_INFINITY;
        }
        logger.info("Create Double Vertex Data Manager: " + fragment.getVerticesNum());
    }

    @Override
    public Double getVertexData(long lid) {
        return values[(int)lid];
    }

    @Override
    public void setVertexData(long lid, Double vertexData) {
        values[(int) lid] = vertexData;
    }
}
