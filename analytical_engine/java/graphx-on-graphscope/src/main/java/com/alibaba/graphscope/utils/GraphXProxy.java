package com.alibaba.graphscope.utils;

import com.alibaba.graphscope.app.GraphXAppBase;
import com.alibaba.graphscope.conf.GraphXConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Function2;
import scala.Function3;

public class GraphXProxy {

    private static Logger logger = LoggerFactory.getLogger(GraphXProxy.class.getName());
    private Function3 vprog;
    private Function1 sendMsg;
    private Function2 mergeMsg;

    public GraphXProxy() {

    }

    public GraphXProxy(GraphXConf conf) {
        Class<? extends GraphXAppBase> clz = conf.getUserAppClass().get();
        GraphXAppBase app;
        try {
            app = clz.newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("Exception in creating graphx App obj");
        }
        vprog = app.vprog();
        sendMsg = app.sendMsg();
        mergeMsg = app.mergeMsg();
        logger.info("Got graphx app instatnce : {}", app);
        logger.info("           vprog: {}", vprog);
        logger.info("           sendMsg: {}", sendMsg);
        logger.info("           mergeMsg: {}", mergeMsg);
    }

}
