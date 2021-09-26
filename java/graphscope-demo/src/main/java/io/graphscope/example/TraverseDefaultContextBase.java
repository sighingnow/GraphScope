package io.graphscope.example;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.grape.ds.EmptyType;
import io.graphscope.context.LabeledVertexDataContext;
import io.graphscope.fragment.ArrowFragment;
import io.graphscope.parallel.PropertyMessageManager;

public class TraverseDefaultContextBase extends LabeledVertexDataContext<Long, EmptyType> {
    public int step;
    public int maxStep;

    public TraverseDefaultContextBase() {
        maxStep = 0;
        step = 0;
    }

    @Override
    public void init(ArrowFragment<Long> fragment, PropertyMessageManager messageManager, JSONObject jsonObject) {
        System.out.println("Enter java context init, args");
        System.out.println(jsonObject.toJSONString());
    }

}
