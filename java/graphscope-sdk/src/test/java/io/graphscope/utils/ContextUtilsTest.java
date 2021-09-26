package io.graphscope.utils;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.grape.fragment.ArrowProjectedFragment;
import com.alibaba.grape.parallel.DefaultMessageManager;
import io.graphscope.context.LabeledVertexDataContext;
import io.graphscope.context.VertexDataContext;
import io.graphscope.fragment.ArrowFragment;
import io.graphscope.parallel.PropertyMessageManager;
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
