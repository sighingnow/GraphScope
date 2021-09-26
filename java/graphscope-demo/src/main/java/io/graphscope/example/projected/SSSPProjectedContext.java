package io.graphscope.example.projected;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.grape.ds.GSVertexArray;
import com.alibaba.grape.ds.VertexRange;
import com.alibaba.grape.ds.VertexSet;
import com.alibaba.grape.fragment.ArrowProjectedFragment;
import com.alibaba.grape.parallel.DefaultMessageManager;
import io.v6d.modules.graph.context.VertexDataContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSSPProjectedContext extends VertexDataContext<ArrowProjectedFragment<Long, Long, Double, Long>, Long> {
    public long sourceOid = -1;
    public GSVertexArray<Long> partialResults;
    public VertexSet curModified;
    public VertexSet nextModified;
    public static Logger logger = LoggerFactory.getLogger(SSSPProjectedContext.class.getName());

    @Override
    public void init(ArrowProjectedFragment<Long, Long, Double, Long> fragment, DefaultMessageManager messageManager, JSONObject jsonObject) {
        createFFIContext(fragment, Long.class, true);
        partialResults = data();
        VertexRange<Long> vertices = fragment.vertices();
        partialResults.Init(vertices, Long.MAX_VALUE);
        curModified = new VertexSet(vertices);
        nextModified = new VertexSet(vertices);

        sourceOid = jsonObject.getLong("src");
        if (!jsonObject.containsKey("src")) {
            logger.error("source Oid not set in parameter.");
            return;
        }
    }
}
