package com.alibaba.graphscope.app;

import com.alibaba.fastffi.FFIByteString;
import com.alibaba.graphscope.communication.Communicator;
import com.alibaba.graphscope.context.DefaultContextBase;
import com.alibaba.graphscope.context.GraphXAdaptorContext;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.graph.GraphXPIE;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphXAdaptor<VDATA_T, EDATA_T, MSG> extends Communicator implements
    DefaultAppBase<Long, Long, VDATA_T, EDATA_T, GraphXAdaptorContext<VDATA_T, EDATA_T, MSG>> {

    private static Logger logger = LoggerFactory.getLogger(GraphXAdaptor.class.getName());
    private static String gsRuntimeJar = "local:/opt/graphscope/lib/grape-runtime-0.1-shaded.jar";
    private static String gsLibPath = "/opt/graphscope/lib";

    /**
     * With this ugly method, we can ensure runtime class infos
     *
     * @return
     */
    public static <VD, ED, M> GraphXAdaptor<VD, ED, M> create(String vdClass, String edClass,
        String msgClass) {
        if (vdClass.equals("int64_t") && edClass.equals("int64_t") && msgClass.equals("int64_t")) {
            return (GraphXAdaptor<VD, ED, M>) new GraphXAdaptor<Long, Long, Long>();
        } else if (vdClass.equals("int64_t") && edClass.equals("int32_t") && msgClass.equals(
            "int64_t")) {
            return (GraphXAdaptor<VD, ED, M>) new GraphXAdaptor<Long, Integer, Long>();
        } else if (vdClass.equals("double") && edClass.equals("int32_t") && msgClass.equals(
            "double")) {
            return (GraphXAdaptor<VD, ED, M>) new GraphXAdaptor<Double, Integer, Double>();
        } else if (vdClass.equals("double") && edClass.equals("double") && msgClass.equals(
            "double")) {
            return (GraphXAdaptor<VD, ED, M>) new GraphXAdaptor<Double, Double, Double>();
        } else if (vdClass.equals("std::string") && edClass.equals("int64_t") && msgClass.equals(
            "double")) {
            return (GraphXAdaptor<VD, ED, M>) new GraphXAdaptor<FFIByteString, Long, Double>();
        } else if (vdClass.equals("std::string") && edClass.equals("double") && msgClass.equals(
            "double")) {
            return (GraphXAdaptor<VD, ED, M>) new GraphXAdaptor<FFIByteString, Double, Double>();
        }
        else {
            throw new IllegalStateException(
                "not supported classes: " + vdClass + "," + edClass + "," + msgClass);
        }
    }

    @Override
    public void PEval(IFragment<Long, Long, VDATA_T, EDATA_T> graph,
        DefaultContextBase<Long, Long, VDATA_T, EDATA_T> context,
        DefaultMessageManager messageManager) {
        GraphXAdaptorContext<VDATA_T, EDATA_T, MSG> ctx = (GraphXAdaptorContext<VDATA_T, EDATA_T, MSG>) context;
        GraphXPIE<VDATA_T, EDATA_T, MSG> proxy = ctx.getGraphXProxy();
//        proxy.init(graph, messageManager, ctx.getInitialMsg());//fix initial msg
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
