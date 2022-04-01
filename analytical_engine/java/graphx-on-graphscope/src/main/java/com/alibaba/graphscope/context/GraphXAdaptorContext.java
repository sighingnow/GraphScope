package com.alibaba.graphscope.context;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.conf.GraphXConf;
import com.alibaba.graphscope.factory.GraphXFactory;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.mm.MessageStore;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.utils.GraphXProxy;
import org.apache.spark.graphx.impl.GraphImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphXAdaptorContext<VDATA_T, EDATA_T> extends
    VertexDataContext<IFragment<Long, Long, VDATA_T, EDATA_T>, VDATA_T> implements
    DefaultContextBase<Long, Long, VDATA_T, EDATA_T> {
    private static Logger logger = LoggerFactory.getLogger(GraphXAdaptorContext.class.getName());
        private static String USER_CLASS = "user_class";
    private String userClassName;
    public String getUserClassName(){return userClassName;}
    /**
     * GraphXProxy manages function objects
     */
//    private GraphXProxy graphXProxy;
//    private GraphXConf conf;
//    public GraphXConf getConf(){
//        return conf;
//    }

//    public GraphXProxy getGraphXProxy(){
//        return graphXProxy;
//    }

    @Override
    public void Init(IFragment<Long, Long, VDATA_T, EDATA_T> frag,
        DefaultMessageManager messageManager, JSONObject jsonObject)  {
        //TODO: get vdata class from conf
        createFFIContext(frag, (Class<? extends VDATA_T>)Double.class, false);

        if (jsonObject.containsKey(USER_CLASS) && !jsonObject.getString(USER_CLASS).isEmpty()) {
            logger.info("Parse user app class {} from json str", jsonObject.getString(USER_CLASS));
            userClassName = jsonObject.getString(USER_CLASS);
//        }
        }
        else {
            throw new IllegalStateException("user class required");
        }
//        graphXProxy = GraphXFactory.createGraphXProxy(conf, messageManager);
        //Fetch graph data from c++, copy to java heap and construct a GraphX graph

    }

    @Override
    public void Output(IFragment<Long, Long, VDATA_T, EDATA_T> frag) {

    }


}
