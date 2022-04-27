package com.alibaba.graphscope.graph.impl;

import com.alibaba.graphscope.conf.GraphXConf;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graph.VertexDataManager;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.clearspring.analytics.util.Lists;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VertexDataManagerImpl<VD> implements VertexDataManager<VD> {
    private static Logger logger = LoggerFactory.getLogger(VertexDataManagerImpl.class.getName());
    private GraphXConf conf;
    private Class<? extends VD> vDataClass;
    private List<VD> values;
    private IFragment<Long,Long,VD,?> iFragment;

    public VertexDataManagerImpl(GraphXConf conf){
        this.conf = conf;
        vDataClass = conf.getVdataClass();
    }

    @Override
    public void setIFragment(IFragment<Long,Long,VD,?> fragment){
        this.iFragment  = fragment;
    }

    @Override
    public void setValues(List<VD> vdatas) {
        this.values = vdatas;
    }

    @Override
    public void init(IFragment<Long, Long, VD, ?> fragment) {
        this.iFragment = fragment;
//        values = (VD[]) new Object[Math.toIntExact((Long) fragment.getVerticesNum())];
        values = new ArrayList<VD>(Math.toIntExact(fragment.getVerticesNum()));
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        long innerVerticesNum = fragment.getInnerVerticesNum();
        for (long lid = 0; lid < innerVerticesNum; ++lid){
            vertex.SetValue(lid);
            values.set((int)lid, fragment.getData(vertex));
//            logger.info("vdata lid [{}] value [{}]", lid, values[(int) lid]);
        }
        //FIXME: ArrowProjectedFragment stores not outer vertex data,
        if (vDataClass.equals(Long.class)){
            for (long lid = innerVerticesNum; lid < values.size(); ++lid) {
                values.set((int) lid, (VD) (Long) Long.MAX_VALUE);
            }
        }
        else if (vDataClass.equals(Double.class)){
            for (long lid = innerVerticesNum; lid < values.size(); ++lid) {
                values.set((int) lid, (VD) (Double) Double.POSITIVE_INFINITY);
            }
        }
        else {
            throw new IllegalStateException("unrecoginized vd class");
        }

        logger.info("Create Vertex Data Manager: " + fragment.getVerticesNum());
    }

    @Override
    public VD getVertexData(long lid) {
        return values.get((int) lid);
    }

    @Override
    public void setVertexData(long lid, VD vertexData) {
        values.set((int) lid, vertexData);
    }


    @Override
    public <VD2> VertexDataManager<VD2> withNewVertexData(List<VD2> newVertexData) {
        VertexDataManager<VD2> newMananger = new VertexDataManagerImpl<VD2>(conf);
        //FIXME: remove vd2.
        newMananger.setIFragment((IFragment<Long, Long, VD2, ?>) this.iFragment);
        newMananger.setValues(newVertexData);
        return newMananger;
    }
}
