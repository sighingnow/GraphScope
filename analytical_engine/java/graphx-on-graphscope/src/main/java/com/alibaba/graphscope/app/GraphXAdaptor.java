package com.alibaba.graphscope.app;

import com.alibaba.graphscope.communication.Communicator;
import com.alibaba.graphscope.context.DefaultContextBase;
import com.alibaba.graphscope.context.GraphXAdaptorContext;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.utils.GraphXProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphXAdaptor<VDATA_T, EDATA_T> extends Communicator implements
    DefaultAppBase<Long, Long, VDATA_T, EDATA_T, GraphXAdaptorContext<VDATA_T, EDATA_T>> {

    private static Logger logger = LoggerFactory.getLogger(GraphXAdaptor.class.getName());
    private static String gsRuntimeJar = "local:/opt/graphscope/lib/grape-runtime-0.1-shaded.jar";
    private static String gsLibPath = "/opt/graphscope/lib";

    @Override
    public void PEval(IFragment<Long, Long, VDATA_T, EDATA_T> graph,
        DefaultContextBase<Long, Long, VDATA_T, EDATA_T> context,
        DefaultMessageManager messageManager) {
        GraphXAdaptorContext<VDATA_T, EDATA_T> ctx = (GraphXAdaptorContext<VDATA_T, EDATA_T>) context;
        GraphXProxy proxy = ctx.getGraphXProxy();
        proxy.init(graph, messageManager, ctx.getInitialMsg());//fix initial msg
        proxy.compute();
        proxy.postApp();
    }

    @Override
    public void IncEval(IFragment<Long, Long, VDATA_T, EDATA_T> graph,
        DefaultContextBase<Long, Long, VDATA_T, EDATA_T> context,
        DefaultMessageManager messageManager) {
        GraphXAdaptorContext<VDATA_T, EDATA_T> ctx = (GraphXAdaptorContext<VDATA_T, EDATA_T>) context;
//        GraphXProxy graphXProxy = ctx.getGraphXProxy();
        //There will be no incEval.
    }
}
