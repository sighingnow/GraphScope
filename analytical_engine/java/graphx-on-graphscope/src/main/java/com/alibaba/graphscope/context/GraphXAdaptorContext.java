package com.alibaba.graphscope.context;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.conf.GraphXConf;
import com.alibaba.graphscope.factory.GraphXFactory;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.utils.GraphXProxy;
import org.apache.spark.graphx.impl.GraphImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphXAdaptorContext<VDATA_T, EDATA_T> extends
    VertexDataContext<IFragment<Long, Long, VDATA_T, EDATA_T>, VDATA_T> implements
    DefaultContextBase<Long, Long, VDATA_T, EDATA_T> {
    private static Logger logger = LoggerFactory.getLogger(GraphXAdaptorContext.class.getName());

    /**
     * GraphXProxy manages function objects
     */
    private GraphXProxy graphXProxy;
    private GraphXConf conf;

    @Override
    public void Init(IFragment<Long, Long, VDATA_T, EDATA_T> frag,
        DefaultMessageManager messageManager, JSONObject jsonObject)  {
        //First check validity of graphx user algorithms
        try {
            conf = GraphXConf.parseFromJson(jsonObject, GraphXAdaptorContext.class.getClassLoader());
        }
        catch (Exception e){
            logger.error("Exception occured in parse from json");
            e.printStackTrace();
            return ;
        }
        graphXProxy = GraphXFactory.createGraphXProxy(conf);


        //Fetch graph data from c++, copy to java heap and construct a GraphX graph

        //Find out the app class from jsonObject.(Pregel interface)

    }

    @Override
    public void Output(IFragment<Long, Long, VDATA_T, EDATA_T> frag) {

    }
}
