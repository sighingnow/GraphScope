package com.alibaba.graphscope.context;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import org.apache.spark.graphx.impl.GraphImpl;

public class GraphXAdaptorContext<VDATA_T, EDATA_T> extends
    VertexDataContext<IFragment<Long, Long, VDATA_T, EDATA_T>, VDATA_T> implements
    DefaultContextBase<Long, Long, VDATA_T, EDATA_T> {

    @Override
    public void Init(IFragment<Long, Long, VDATA_T, EDATA_T> frag,
        DefaultMessageManager messageManager, JSONObject jsonObject) {

        //Fetch graph data from c++, copy to java heap and construct a GraphX graph

        //Find out the app class from jsonObject.(Pregel interface)

    }

    @Override
    public void Output(IFragment<Long, Long, VDATA_T, EDATA_T> frag) {

    }
}
