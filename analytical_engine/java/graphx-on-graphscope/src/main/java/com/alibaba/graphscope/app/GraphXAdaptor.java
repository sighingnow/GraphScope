package com.alibaba.graphscope.app;

import com.alibaba.graphscope.communication.Communicator;
import com.alibaba.graphscope.conf.GraphXConf;
import com.alibaba.graphscope.context.DefaultContextBase;
import com.alibaba.graphscope.context.GraphXAdaptorContext;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.utils.GraphConverter;
import com.alibaba.graphscope.utils.GraphXProxy;
import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Duration;
import org.apache.spark.graphx.Pregel;
import org.apache.spark.launcher.InProcessLauncher;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.Listener;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphXAdaptor<VDATA_T,EDATA_T> extends Communicator implements DefaultAppBase<Long,Long,VDATA_T,EDATA_T, GraphXAdaptorContext<VDATA_T,EDATA_T>>{
    private static Logger logger = LoggerFactory.getLogger(GraphXAdaptor.class.getName());
    private static String gsRuntimeJar = "local:/opt/graphscope/lib/grape-runtime-0.1-shaded.jar";
    private static String gsLibPath = "/opt/graphscope/lib";
    @Override
    public void PEval(IFragment<Long, Long, VDATA_T, EDATA_T> graph,
        DefaultContextBase<Long, Long, VDATA_T, EDATA_T> context,
        DefaultMessageManager messageManager) {
        GraphXAdaptorContext<VDATA_T,EDATA_T> ctx = (GraphXAdaptorContext<VDATA_T, EDATA_T>) context;
        try {
            //Set communicator to Pregel Class static field.
            Pregel.setCommunicator((Communicator) this);
            Pregel.setMessageManager(messageManager);
            Thread.currentThread().setContextClassLoader();
            invokeMain(ctx.getUserClassName());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void IncEval(IFragment<Long, Long, VDATA_T, EDATA_T> graph,
        DefaultContextBase<Long, Long, VDATA_T, EDATA_T> context,
        DefaultMessageManager messageManager) {
        GraphXAdaptorContext<VDATA_T,EDATA_T> ctx = (GraphXAdaptorContext<VDATA_T, EDATA_T>) context;
//        GraphXProxy graphXProxy = ctx.getGraphXProxy();
        //There will be no incEval.
    }

    /**
     * Invoking main function of user scala app. This will do
     *     1) Start spark inprocess
     *     2) create graphxProxy in scala
     *     3) create graphConverter in scala
     *     4) convert graphx-Graph to fragment, and kick-off computation in graphx proxy.
     * @param userClassName userClassName
     * @throws IOException
     * @throws InterruptedException
     */
    public void invokeMain(String userClassName) throws IOException, InterruptedException {
        //lauch with spark laucher
        String user_jar_path = System.getenv("USER_JAR_PATH");
        if (user_jar_path == null || user_jar_path.isEmpty()) {
            logger.error("USER_JAR_PATH not set");
        }
        String javaLibraryPath = System.getProperty("java.library.path");
        String javaClassPath = System.getProperty("java.class.path");
        logger.info("java.library.path {}, java.class.path {}", javaLibraryPath, javaClassPath);
        logger.info("user app class: " + userClassName);
        String jars = System.getenv("EXTRA_JARS");
        if (jars == null) {
            jars = gsRuntimeJar;
        }
        SparkAppHandle appHandle = new InProcessLauncher()
            .setAppResource(user_jar_path)
            .setMainClass(userClassName)
            .setMaster("local[2]")
            .setConf(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH, gsRuntimeJar)
            .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, gsRuntimeJar)
            .setConf(SparkLauncher.EXECUTOR_EXTRA_LIBRARY_PATH, gsLibPath)
            .setConf(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH, gsLibPath)
            .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
            .addJar(jars)
            .setVerbose(true).startApplication();
        // Use handle API to monitor / control application.
        appHandle.addListener(new Listener() {
            @Override
            public void stateChanged(SparkAppHandle sparkAppHandle) {
                logger.info("{} staged changed to {}", sparkAppHandle.getAppId(),
                    sparkAppHandle.getState());
            }

            @Override
            public void infoChanged(SparkAppHandle sparkAppHandle) {
                logger.info("info changed");
            }
        });
        logger.info("Start waiting app handle");
        try {
            waitFor(appHandle);
        } catch (Exception e) {
            logger.error("Exception when waiting apphanle to finish");
            e.printStackTrace();
        }
        logger.info("waiting finished");
    }
    private void waitFor(SparkAppHandle handle) throws Exception {
        try {
            eventually(Duration.ofSeconds(10), Duration.ofMillis(10), () -> {
                if (!handle.getState().isFinal()) {
                    throw new AssertionError("Handle is not in the final state");
                }
            });
        } finally {
            if (!handle.getState().isFinal()) {
                handle.kill();
            }
        }
    }
    /**
     * Call a closure that performs a check every "period" until it succeeds, or the timeout
     * elapses.
     */
    protected void eventually(Duration timeout, Duration period, Runnable check)
        throws InterruptedException {
        if (timeout.compareTo(period) < 0) {
            throw new IllegalStateException("Timeout needs to be larger than period.");
        }
        long deadline = System.nanoTime() + timeout.toNanos();
        int count = 0;
        while (true) {
            try {
                count++;
                check.run();
                return;
            } catch (Throwable t) {
                if (System.nanoTime() >= deadline) {
                    String msg = String.format("Failed check after %d tries: %s.", count,
                        t.getMessage());
                    throw new IllegalStateException(msg, t);
                }
                logger.debug("Error catch, continue waiting: " + t.getMessage());
                Thread.sleep(period.toMillis());
            }
        }
    }
}
