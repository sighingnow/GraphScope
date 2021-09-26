package io.graphscope.utils;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.grape.ds.Vertex;
import com.alibaba.grape.utils.FFITypeFactoryhelper;
import io.graphscope.app.PropertyDefaultAppBase;
import io.graphscope.context.LabeledVertexDataContext;
import io.graphscope.context.PropertyDefaultContextBase;
import io.graphscope.fragment.ArrowFragment;
import io.graphscope.parallel.PropertyMessageManager;
import org.junit.Assert;
import org.junit.Test;

public class AppContextGetterTest {
    public static class SampleContext extends LabeledVertexDataContext<Long, Double> {
        public SampleContext() {

        }

        @Override
        public void init(ArrowFragment<Long> fragment, PropertyMessageManager messageManager, JSONObject jsonObject) {
        }
    }

    public static class SampleApp implements PropertyDefaultAppBase<Long, SampleContext> {
        @Override
        public void PEval(ArrowFragment<Long> fragment, PropertyDefaultContextBase<Long> context, PropertyMessageManager messageManager) {
        }

        @Override
        public void IncEval(ArrowFragment<Long> graph, PropertyDefaultContextBase<Long> context, PropertyMessageManager messageManager) {
        }
    }

    @Test
    public void test() {
        Class<? extends PropertyDefaultAppBase> appClass = SampleApp.class;
//        Class<?> ctxClass = AppContextGetter.getPropertyDefaultContext(appClass);
        try {
//            Assert.assertTrue(ctxClass.newInstance() instanceof PropertyDefaultContextBase);
            System.out.println(AppContextGetter.getContextName(appClass));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test1() {
        SampleContext sampleContext = new SampleContext();
        Assert.assertTrue(AppContextGetter.getLabeledVertexDataContextDataType(sampleContext).equals("double"));
    }

    @Test
    public void test2() {
        Vertex<Long> prev = FFITypeFactoryhelper.newVertexLong();
        System.out.println("Vertex<Long>: " + FFITypeFactoryhelper.getForeignName(prev.getClass()));
    }
}
