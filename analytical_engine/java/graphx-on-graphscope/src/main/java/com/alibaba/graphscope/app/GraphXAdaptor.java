package com.alibaba.graphscope.app;

import com.alibaba.graphscope.communication.Communicator;
import com.alibaba.graphscope.context.DefaultContextBase;
import com.alibaba.graphscope.context.GraphXAdaptorContext;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graphx.GraphXPIE;
import com.alibaba.graphscope.graphx.utils.SerializationUtils;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import java.net.URLClassLoader;

public class GraphXAdaptor<VDATA_T, EDATA_T, MSG> implements
    DefaultAppBase<Long, Long, VDATA_T, EDATA_T, GraphXAdaptorContext<VDATA_T, EDATA_T, MSG>> {

    public static <VD, ED, M> GraphXAdaptor<VD, ED, M> createImpl(
        Class<? extends VD> vdClass, Class<? extends ED> edClass,
        Class<? extends M> msgClass) {
        return new GraphXAdaptor<VD, ED, M>();
    }

    public static <VD, ED, M> GraphXAdaptor<VD, ED, M> create(URLClassLoader classLoader,
        String serialPath)
        throws ClassNotFoundException {
        Object[] objects = SerializationUtils.read(classLoader, serialPath);
        if (objects.length != 9) {
            throw new IllegalStateException(
                "Expect 9 deserialzed object, but only got " + objects.length);
        }
        Class<?> vdClass = (Class<?>) objects[0];
        Class<?> edClass = (Class<?>) objects[1];
        Class<?> msgClass = (Class<?>) objects[2];
        return (GraphXAdaptor<VD, ED, M>) createImpl(vdClass, edClass, msgClass);
    }

    @Override
    public void PEval(IFragment<Long, Long, VDATA_T, EDATA_T> graph,
        DefaultContextBase<Long, Long, VDATA_T, EDATA_T> context,
        DefaultMessageManager messageManager) {
        GraphXAdaptorContext<VDATA_T, EDATA_T, MSG> ctx = (GraphXAdaptorContext<VDATA_T, EDATA_T, MSG>) context;
        GraphXPIE<VDATA_T, EDATA_T, MSG> proxy = ctx.getGraphXProxy();
//        proxy.PEval();
        proxy.ParallelPEval();
        messageManager.ForceContinue();
    }

    @Override
    public void IncEval(IFragment<Long, Long, VDATA_T, EDATA_T> graph,
        DefaultContextBase<Long, Long, VDATA_T, EDATA_T> context,
        DefaultMessageManager messageManager) {
        GraphXAdaptorContext<VDATA_T, EDATA_T, MSG> ctx = (GraphXAdaptorContext<VDATA_T, EDATA_T, MSG>) context;
        //if (ctx.round > 5) return ;
        GraphXPIE<VDATA_T, EDATA_T, MSG> proxy = ctx.getGraphXProxy();
//        boolean maxIterationReached = proxy.IncEval();
        boolean maxIterationReached = proxy.ParallelIncEval();
        if (!maxIterationReached) {
            messageManager.ForceContinue();
        }
    }
}
