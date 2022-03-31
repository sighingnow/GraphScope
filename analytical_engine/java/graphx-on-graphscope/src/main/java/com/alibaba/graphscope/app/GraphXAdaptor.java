package com.alibaba.graphscope.app;

import com.alibaba.graphscope.context.DefaultContextBase;
import com.alibaba.graphscope.context.GraphXAdaptorContext;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.utils.GraphConverter;
import com.alibaba.graphscope.utils.GraphXProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphXAdaptor<VDATA_T,EDATA_T> implements DefaultAppBase<Long,Long,VDATA_T,EDATA_T, GraphXAdaptorContext<VDATA_T,EDATA_T>>{
    private static Logger logger = LoggerFactory.getLogger(GraphXAdaptor.class.getName());

    @Override
    public void PEval(IFragment<Long, Long, VDATA_T, EDATA_T> graph,
        DefaultContextBase<Long, Long, VDATA_T, EDATA_T> context,
        DefaultMessageManager messageManager) {
        GraphXAdaptorContext<VDATA_T,EDATA_T> ctx = (GraphXAdaptorContext<VDATA_T, EDATA_T>) context;
        GraphXProxy graphXProxy = ctx.getGraphXProxy();

        //do the computation and message sending
        //flush messages.
//        graphXProxy.compute(graph.innerVertices());
        long tmp = GraphConverter.createArrowFragmentLoader();
        logger.info("created fragment loader: {}", tmp);

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
