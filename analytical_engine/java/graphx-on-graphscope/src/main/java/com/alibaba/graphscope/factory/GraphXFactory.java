package com.alibaba.graphscope.factory;

import com.alibaba.graphscope.conf.GraphXConf;
import com.alibaba.graphscope.utils.GraphXProxy;

/**
 * Providing factory methods for creating adaptor and adptor context.
 */
public class GraphXFactory {
    public static GraphXProxy createGraphXProxy(GraphXConf conf){
        return new GraphXProxy(conf);
    }

}
