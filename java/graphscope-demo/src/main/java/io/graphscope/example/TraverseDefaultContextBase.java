package io.graphscope.example;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.grape.ds.EmptyType;
import io.v6d.modules.graph.context.LabeledVertexDataContext;
import io.v6d.modules.graph.fragment.ArrowFragment;
import io.v6d.modules.graph.parallel.PropertyMessageManager;

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
