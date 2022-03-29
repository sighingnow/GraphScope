package com.alibaba.graphscope.utils;

import com.alibaba.graphscope.app.GraphXAppBase;
import com.alibaba.graphscope.conf.GraphXConf;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.factory.GraphXFactory;
import com.alibaba.graphscope.graph.EdgeContextImpl;
import com.alibaba.graphscope.graph.EdgeManager;
import com.alibaba.graphscope.graph.IdManager;
import com.alibaba.graphscope.graph.VertexDataManager;
import com.alibaba.graphscope.mm.MessageStore;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.spark.graphx.EdgeTriplet;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Function2;
import scala.Function3;
import scala.Tuple2;
import scala.collection.Iterator;

public class GraphXProxy<VD, ED, MSG_T> {

    private static Logger logger = LoggerFactory.getLogger(GraphXProxy.class.getName());
    private static String SPARK_LAUNCHER_OUTPUT = "spark_laucher_output";
    /**
     * User vertex program: vprog: (VertexId, VD, A) => VD
     */
    private Function3<Long, VD, MSG_T, VD> vprog;
    /**
     * EdgeTriplet[VD, ED] => Iterator[(VertexId, A)]
     */
    private Function1<EdgeTriplet<VD, ED>, Iterator<Tuple2<Long, MSG_T>>> sendMsg;
    /**
     * (A, A) => A)
     */
    private Function2<MSG_T, MSG_T, MSG_T> mergeMsg;
    private IdManager idManager;
    private VertexDataManager<VD> vertexDataManager;
    private MessageStore<MSG_T> inComingMessageStore, outgoingMessageStore;
    private EdgeContextImpl<VD, ED, MSG_T> edgeContext;
    private GraphXConf conf;
    private EdgeManager<ED> edgeManager;

    public GraphXProxy(GraphXConf conf, IdManager manager,
        VertexDataManager<VD> vertexDataManager) {
        this.conf = conf;
        this.idManager = manager;
        this.vertexDataManager = vertexDataManager;
        inComingMessageStore = GraphXFactory.createMessageStore(conf, idManager.innerVerticesNum());
        outgoingMessageStore = GraphXFactory.createMessageStore(conf, idManager.verticesNum());
        edgeContext = GraphXFactory.createEdgeContext(conf, outgoingMessageStore);
        edgeManager = GraphXFactory.createEdgeManager(conf);
    }

    public void compute(VertexRange<Long> vertices) {
        long rightMost = vertices.endValue();
        for (long lid = 0; lid < rightMost; ++lid) {
            if (inComingMessageStore.messageAvailable(lid)) {
                vprog.apply(idManager.lid2Oid(lid), vertexDataManager.getVertexData(lid),
                    inComingMessageStore.getMessage(lid));
            }
        }
        //after running vprog, we now send msg and merge msg
        for (long lid = 0; lid < rightMost; ++lid) {
            edgeContext.setSrcValues(idManager.lid2Oid(lid), lid,
                vertexDataManager.getVertexData(lid));
            edgeManager.iterateOnEdges(lid, edgeContext, sendMsg, mergeMsg);
        }
        //after send message, flush message in message manager.
    }

    public void invokeMain() throws IOException, InterruptedException {
        //lauch with spark laucher
        String user_jar_path = System.getenv("USER_JAR_PATH");
        if (user_jar_path == null || user_jar_path.isEmpty()){
            logger.error("USER_JAR_PATH not set");
        }
        Process process = new SparkLauncher()
            .setAppResource(user_jar_path)
            .setMainClass(conf.getUserAppClass().get().getName())
            .setMaster("local")
            .setConf(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH, user_jar_path)
            .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, user_jar_path)
            .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
            .redirectOutput(new File(SPARK_LAUNCHER_OUTPUT))
            .redirectError()
            .launch();
        // Use handle API to monitor / control application.
        logger.info("Start waiting for process {}", process);
        int res= process.waitFor();
        logger.info("waiting res: {}", res);
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

        //get the main function

    }

}
