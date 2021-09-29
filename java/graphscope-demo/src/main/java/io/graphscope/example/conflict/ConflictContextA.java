package io.graphscope.example.conflict;

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

public class ConflictContextA extends LabeledVertexPropertyContext<Long> {
    public static int flag = 1;
    public static Logger logger = LoggerFactory.getLogger(ConflictContextA.class.getName());

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

