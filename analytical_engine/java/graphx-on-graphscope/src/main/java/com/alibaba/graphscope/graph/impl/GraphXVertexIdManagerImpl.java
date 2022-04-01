package com.alibaba.graphscope.graph.impl;

import com.alibaba.graphscope.conf.GraphXConf;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graph.GraphXVertexIdManager;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphXVertexIdManagerImpl implements GraphXVertexIdManager {

    private static Logger logger = LoggerFactory.getLogger(GraphXVertexIdManagerImpl.class.getName());

    private GraphXConf conf;
    private Long[] oids;
    private long innerVerticesNum, fragVerticesNum;
    private Long2LongOpenHashMap oid2Lid;

    public GraphXVertexIdManagerImpl(GraphXConf conf) {
        this.conf = conf;
    }

    @Override
    public void init(IFragment<Long, Long, ?, ?> fragment) {
        //all frag vertices;
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        for (long lid = 0; lid < fragment.getVerticesNum(); ++lid) {
            vertex.SetValue(lid);
            long oid = fragment.getId(vertex);
            oids[(int) lid] = oid;
            oid2Lid.put(oid, lid);
        }
        logger.info("Finish VertexId manager construction: fragId [{}], vertices [{}]", fragment.fid(), fragment.getVerticesNum());
        innerVerticesNum = fragment.getInnerVerticesNum();
        fragVerticesNum = fragment.getVerticesNum();
    }

    @Override
    public Long lid2Oid(Long lid) {
        return oids[lid.intValue()];
    }

    @Override
    public Long oid2Lid(Long oid){
        return oid2Lid.getOrDefault(oid, -1L);
    }

    @Override
    public long innerVerticesNum() {
        return innerVerticesNum;
    }

    @Override
    public long verticesNum() {
        return fragVerticesNum;
    }
}
