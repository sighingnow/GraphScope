package com.alibaba.graphscope.graphx;

import com.alibaba.graphscope.graph.GraphXVertexIdManager;
import com.alibaba.graphscope.graph.GraphxEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JavaEdgePartition<VD, ED> {

    private Logger logger = LoggerFactory.getLogger(JavaEdgePartition.class.getName());
    private GraphXVertexIdManager vertexIdManager;
    private GraphxEdgeManager<VD, ED, ?> edgeManager;
    private int pid;
    private long startLid, endLid;

    public JavaEdgePartition(int pid, int numPartitions, GraphXVertexIdManager idManager,
        GraphxEdgeManager<VD, ED, ?> edgeManager) {
        this.pid = pid;
        this.vertexIdManager = idManager;
        long totalVnum = idManager.innerVerticesNum();
        long chunkSize = (totalVnum + (numPartitions - 1)) / numPartitions;
        this.startLid = Math.min(chunkSize * pid, totalVnum);
        this.endLid = Math.min(startLid + chunkSize, totalVnum);
        this.edgeManager = edgeManager;
        logger.info("Creating JavaEdgePartition {}", this);
    }

    @Override
    public String toString() {
        return "JavaEdgePartition{" +
            "vertexIdManager=" + vertexIdManager +
            ", edgeManager=" + edgeManager +
            ", pid=" + pid +
            ", startLid=" + startLid +
            ", endLid=" + endLid +
            '}';
    }


}
