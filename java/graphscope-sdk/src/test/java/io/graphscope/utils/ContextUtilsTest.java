package io.v6d.modules.graph.utils;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.grape.fragment.ArrowProjectedFragment;
import com.alibaba.grape.parallel.DefaultMessageManager;
import io.v6d.modules.graph.context.LabeledVertexDataContext;
import io.v6d.modules.graph.context.VertexDataContext;
import io.v6d.modules.graph.fragment.ArrowFragment;
import io.v6d.modules.graph.parallel.PropertyMessageManager;
import org.junit.Assert;
import org.junit.Test;

public class ContextUtilsTest {

    public static class SampleContext extends LabeledVertexDataContext<Long, Double> {
        @Override
        public void init(ArrowFragment<Long> fragment, PropertyMessageManager messageManager, JSONObject jsonObject) {
        }
    }

    public static class SampleContext2 extends VertexDataContext<ArrowProjectedFragment<Long, Long, Long, Double>, Double> {
        @Override
        public void init(ArrowProjectedFragment<Long, Long, Long, Double> fragment, DefaultMessageManager messageManager, JSONObject jsonObject) {

        }
    }

    @Test
    public void test() {
        Assert.assertTrue(ContextUtils.getPropertyCtxObjBaseClzName(new SampleContext()).equals("LabeledVertexDataContext"));
        Assert.assertTrue(ContextUtils.getProjectedCtxObjBaseClzName(new SampleContext2()).equals("VertexDataContext"));
    }
}
