package io.graphscope.example.conflict;

import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.ds.VertexRange;
import com.alibaba.grape.parallel.message.DoubleMsg;
import com.alibaba.grape.utils.FFITypeFactoryhelper;
import io.graphscope.app.PropertyDefaultAppBase;
import io.graphscope.column.DoubleColumn;
import io.graphscope.context.PropertyDefaultContextBase;
import io.graphscope.ds.PropertyAdjList;
import io.graphscope.ds.PropertyNbr;
import io.graphscope.example.sssp.PropertySSSPContext;
import io.graphscope.fragment.ArrowFragment;
import io.graphscope.parallel.PropertyMessageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConflictA implements PropertyDefaultAppBase<Long, ConflictContextA> {
    public static int flag = 1;
    public static Logger logger = LoggerFactory.getLogger(ConflictA.class.getName());

    @Override
    public void PEval(ArrowFragment<Long> fragment, PropertyDefaultContextBase<Long> context, PropertyMessageManager messageManager) {
        logger.info("Static flag: " + flag);
    }

    @Override
    public void IncEval(ArrowFragment<Long> fragment, PropertyDefaultContextBase<Long> context, PropertyMessageManager messageManager) {

    }
}

