package io.graphscope.example.sssp;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.grape.ds.GSVertexArray;
import com.alibaba.grape.ds.VertexRange;
import com.alibaba.grape.ds.VertexSet;
import com.alibaba.grape.utils.FFITypeFactoryhelper;
import io.graphscope.context.ContextDataType;
import io.graphscope.context.LabeledVertexPropertyContext;
import io.graphscope.fragment.ArrowFragment;
import io.graphscope.parallel.PropertyMessageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PropertySSSPContext extends LabeledVertexPropertyContext<Long> {
    public List<VertexSet> curModified;
    public List<VertexSet> nextModified;
    public List<GSVertexArray<Double>> partialResults;
    public List<Long> distColumnIndices;
    public long sourceOid;
    public static Logger logger = LoggerFactory.getLogger(PropertySSSPContext.class.getName());

    /**
     * @param fragment
     * @param messageManager
     * @param jsonObject     contains the user-defined parameters in json manner
     */
    @Override
    public void init(ArrowFragment<Long> fragment, PropertyMessageManager messageManager, JSONObject jsonObject) {
        //must be called
        createFFIContext(fragment);
        logger.info("params size " + jsonObject.size() + ", " + jsonObject.toJSONString());
        int labelNum = fragment.vertexLabelNum();

        curModified = new ArrayList<>(labelNum);
        nextModified = new ArrayList<>(labelNum);
        partialResults = new ArrayList<>(labelNum);
        distColumnIndices = new ArrayList<>(labelNum);
        for (int i = 0; i < labelNum; ++i) {
            VertexRange<Long> vertices = fragment.vertices(i);
            curModified.add(new VertexSet(vertices));
            nextModified.add(new VertexSet(vertices));
            partialResults.add(FFITypeFactoryhelper.newGSVertexArray(Double.class));
            partialResults.get(i).Init(fragment.vertices(i), Double.MAX_VALUE);
            distColumnIndices.add(addColumn(i, "dist_" + i, ContextDataType.kDouble));
            logger.info("range " + partialResults.get(i).GetVertexRange().begin().GetValue() +
                    ", " + partialResults.get(i).GetVertexRange().end().GetValue());
        }
        sourceOid = jsonObject.getLong("src");
        if (Objects.isNull(sourceOid)) {
            logger.error("source Oid not set in parameter.");
            return;
        }
    }
}
