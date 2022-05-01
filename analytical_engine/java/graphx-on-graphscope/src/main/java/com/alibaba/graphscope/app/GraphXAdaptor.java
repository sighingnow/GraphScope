package com.alibaba.graphscope.app;

import com.alibaba.graphscope.communication.Communicator;
import com.alibaba.graphscope.context.DefaultContextBase;
import com.alibaba.graphscope.context.GraphXAdaptorContext;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import org.apache.spark.graphx.impl.graph.GraphXProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphXAdaptor<VDATA_T, EDATA_T,MSG> extends Communicator implements
    DefaultAppBase<Long, Long, VDATA_T, EDATA_T, GraphXAdaptorContext<VDATA_T, EDATA_T,MSG>> {

    private static Logger logger = LoggerFactory.getLogger(GraphXAdaptor.class.getName());
    private static String gsRuntimeJar = "local:/opt/graphscope/lib/grape-runtime-0.1-shaded.jar";
    private static String gsLibPath = "/opt/graphscope/lib";

    public static <VD,ED,M> GraphXAdaptor<VD,ED,M> create(String vdClass, String edClass, String msgClass){
        if (vdClass.equals("int64_t") && edClass.equals("int64_t") && msgClass.equals("int64_t")){
            return (GraphXAdaptor<VD, ED, M>) new GraphXAdaptor<Long, Long, Long>();
        }
        else throw new IllegalStateException("not supported classes: " + vdClass + "," +  edClass + "," +  msgClass);
    }

    @Override
    public void PEval(IFragment<Long, Long, VDATA_T, EDATA_T> graph,
        DefaultContextBase<Long, Long, VDATA_T, EDATA_T> context,
        DefaultMessageManager messageManager) {
        GraphXAdaptorContext<VDATA_T, EDATA_T,MSG> ctx = (GraphXAdaptorContext<VDATA_T, EDATA_T,MSG>) context;
        GraphXProxy proxy = ctx.getGraphXProxy();
//        proxy.init(graph, messageManager, ctx.getInitialMsg());//fix initial msg
        proxy.ParallelPEval();
        messageManager.ForceContinue();
    }

    @Override
    public void IncEval(IFragment<Long, Long, VDATA_T, EDATA_T> graph,
        DefaultContextBase<Long, Long, VDATA_T, EDATA_T> context,
        DefaultMessageManager messageManager) {
        GraphXAdaptorContext<VDATA_T, EDATA_T,MSG> ctx = (GraphXAdaptorContext<VDATA_T, EDATA_T,MSG>) context;
	//if (ctx.round > 5) return ;
        GraphXProxy proxy = ctx.getGraphXProxy();
        boolean maxIterationReached = proxy.ParallelIncEval();
        if (!maxIterationReached && proxy.getOutgoingMessageStore().hasMessages()){
            messageManager.ForceContinue();
        }
    }
}
