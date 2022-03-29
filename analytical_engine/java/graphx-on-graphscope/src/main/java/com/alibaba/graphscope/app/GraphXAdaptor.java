package com.alibaba.graphscope.app;

import com.alibaba.graphscope.context.DefaultContextBase;
import com.alibaba.graphscope.context.GraphXAdaptorContext;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.utils.GraphXProxy;

public class GraphXAdaptor<VDATA_T,EDATA_T> implements DefaultAppBase<Long,Long,VDATA_T,EDATA_T, GraphXAdaptorContext<VDATA_T,EDATA_T>>{

    @Override
    public void PEval(IFragment<Long, Long, VDATA_T, EDATA_T> graph,
        DefaultContextBase<Long, Long, VDATA_T, EDATA_T> context,
        DefaultMessageManager messageManager) {
        GraphXAdaptorContext<VDATA_T,EDATA_T> ctx = (GraphXAdaptorContext<VDATA_T, EDATA_T>) context;
        GraphXProxy graphXProxy = ctx.getGraphXProxy();

        //do the computation and message sending
        //flush messages.
//        graphXProxy.compute(graph.innerVertices());

        try{
            graphXProxy.invokeMain();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void IncEval(IFragment<Long, Long, VDATA_T, EDATA_T> graph,
        DefaultContextBase<Long, Long, VDATA_T, EDATA_T> context,
        DefaultMessageManager messageManager) {
        GraphXAdaptorContext<VDATA_T,EDATA_T> ctx = (GraphXAdaptorContext<VDATA_T, EDATA_T>) context;
        GraphXProxy graphXProxy = ctx.getGraphXProxy();

        //do the computation and message sending
        //flush messages.
//        graphXProxy.compute(graph.innerVertices());
    }
}
