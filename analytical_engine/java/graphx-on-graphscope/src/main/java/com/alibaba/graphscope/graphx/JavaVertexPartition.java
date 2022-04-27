package com.alibaba.graphscope.graphx;

import com.alibaba.graphscope.graph.GraphXVertexIdManager;
import com.alibaba.graphscope.graph.VertexDataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JavaVertexPartition<VD> {
    private static Logger logger = LoggerFactory.getLogger(JavaVertexPartition.class.getName());
    private GraphXVertexIdManager idManager;//multiple partitions share the same id manager;
    private VertexDataManager<VD> vertexDataManager;
    private long startLid,endLid;
    private int pid;

    public JavaVertexPartition(int pid,int numPartitions, GraphXVertexIdManager idManager, VertexDataManager<VD> vertexDataManager){
        this.idManager = idManager;
        this.vertexDataManager = vertexDataManager;
        this.pid = pid;
        long totalVnum = idManager.innerVerticesNum();
        long chunkSize = (totalVnum + (numPartitions - 1)) / numPartitions;
        this.startLid = Math.min(chunkSize * pid, totalVnum);
        this.endLid = Math.min(startLid + chunkSize, totalVnum);
        logger.info("Creating JavaVertexPartition {}", this);
    }


    @Override
    public String toString() {
        return "JavaVertexPartition{" +
            "idManager=" + idManager +
            ", vertexDataManager=" + vertexDataManager +
            ", startLid=" + startLid +
            ", endLid=" + endLid +
            ", pid=" + pid +
            '}';
    }
}
