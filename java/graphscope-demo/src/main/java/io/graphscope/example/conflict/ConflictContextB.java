package io.graphscope.example.conflict;

import com.alibaba.fastjson.JSONObject;
import io.graphscope.context.LabeledVertexPropertyContext;
import io.graphscope.fragment.ArrowFragment;
import io.graphscope.parallel.PropertyMessageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConflictContextB extends LabeledVertexPropertyContext<Long> {
    public static int flag = 2;
    public static Logger logger = LoggerFactory.getLogger(ConflictContextB.class.getName());

    /**
     * @param fragment
     * @param messageManager
     * @param jsonObject     contains the user-defined parameters in json manner
     */
    @Override
    public void init(ArrowFragment<Long> fragment, PropertyMessageManager messageManager, JSONObject jsonObject) {
        //must be called
        createFFIContext(fragment);
        logger.info("Static var: " + flag);
    }
}
