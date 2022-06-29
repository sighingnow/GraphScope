package com.alibaba.graphscope.app;

import com.alibaba.fastffi.FFIByteString;
import com.alibaba.graphscope.communication.Communicator;
import com.alibaba.graphscope.context.DefaultContextBase;
import com.alibaba.graphscope.context.GraphXAdaptorContext;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graph.GraphXPIE;
import com.alibaba.graphscope.graphx.SerializationUtils;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import java.net.URLClassLoader;
import org.apache.spark.graphx.EdgeTriplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Function2;
import scala.Function3;
import scala.Tuple2;
import scala.collection.Iterator;

public class GraphXAdaptor<VDATA_T, EDATA_T, MSG> extends Communicator implements
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
        if (objects.length != 8) {
            throw new IllegalStateException(
                "Expect 7 deserialzed object, but only got " + objects.length);
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
        proxy.PEval();
        messageManager.ForceContinue();
    }

    @Override
    public void IncEval(IFragment<Long, Long, VDATA_T, EDATA_T> graph,
        DefaultContextBase<Long, Long, VDATA_T, EDATA_T> context,
        DefaultMessageManager messageManager) {
        GraphXAdaptorContext<VDATA_T, EDATA_T, MSG> ctx = (GraphXAdaptorContext<VDATA_T, EDATA_T, MSG>) context;
        //if (ctx.round > 5) return ;
        GraphXPIE<VDATA_T, EDATA_T, MSG> proxy = ctx.getGraphXProxy();
        boolean maxIterationReached = proxy.IncEval();
        if (!maxIterationReached) {
            messageManager.ForceContinue();
        }
    }
}
