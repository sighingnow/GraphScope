package io.graphscope.example.conflict;

import io.graphscope.app.PropertyDefaultAppBase;
import io.graphscope.context.PropertyDefaultContextBase;
import io.graphscope.example.sssp.PropertySSSPContext;
import io.graphscope.fragment.ArrowFragment;
import io.graphscope.parallel.PropertyMessageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConflictB implements PropertyDefaultAppBase<Long, ConflictContextB> {
    public static int flag = 2;
    public static Logger logger = LoggerFactory.getLogger(ConflictB.class.getName());

    @Override
    public void PEval(ArrowFragment<Long> fragment, PropertyDefaultContextBase<Long> context, PropertyMessageManager messageManager) {
        logger.info("Static flag: " + flag);
    }

    @Override
    public void IncEval(ArrowFragment<Long> fragment, PropertyDefaultContextBase<Long> context, PropertyMessageManager messageManager) {
    }
}
