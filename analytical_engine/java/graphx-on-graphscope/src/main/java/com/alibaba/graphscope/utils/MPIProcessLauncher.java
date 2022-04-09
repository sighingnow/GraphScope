package com.alibaba.graphscope.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executed on driver node, create several mpi processes.
 */
public class MPIProcessLauncher<VD,ED,MSG> {

    private static Logger logger = LoggerFactory.getLogger(MPIProcessLauncher.class.getName());

    private Integer numWorker = 1;
    private String vertexFilePrefix, edgeFilePrefix, vprogPrefix, sendMsgPrefix, mergeMsgPrefix;
    private ProcessBuilder processBuilder;
    private static String GRAPHSCOPE_CODE_HOME, SPARK_HOME, GAE_HOME, SPARK_CONF_WORKERS;
    private static String MPI_EXEC = "mpirun";
    private static String SHELL_SCRIPT;
    private String userClass;
    private Class<? extends VD> vdClass;
    private Class<? extends ED> edClass;
    private Class<? extends MSG> msgClass;

    static {
        SPARK_HOME = System.getenv("SPARK_HOME");
        if (SPARK_HOME == null || SPARK_HOME.isEmpty()) {
            throw new IllegalStateException("SPARK_HOME need");
        }
        SPARK_CONF_WORKERS = SPARK_HOME + "/conf/workers";

        GRAPHSCOPE_CODE_HOME = System.getenv("GRAPHSCOPE_CODE_HOME");
        if (GRAPHSCOPE_CODE_HOME != null && fileExists(GRAPHSCOPE_CODE_HOME)) {
            GAE_HOME = GRAPHSCOPE_CODE_HOME + "/analytical_engine";
            if (!fileExists(GAE_HOME)) {
                throw new IllegalStateException("GAE HOME wrong" + GAE_HOME);
            }
        } else {
            throw new IllegalStateException("GraphScope code home wrong");
        }
        SHELL_SCRIPT = GAE_HOME + "/java/launch_mpi.sh";
        if (!fileExists(SHELL_SCRIPT)) {
            throw new IllegalStateException("script " + GAE_HOME + "doesn't exist");
        }
    }

    public MPIProcessLauncher(String vertexFilePrefix, String edgeFilePrefix, String vprogPrefix,
        String sendMsgPrefix, String mergeMsgPrefix, String userClass, Class<? extends VD> vdClass, Class<? extends ED> edClass, Class<? extends MSG> msgClass) {
        this.vertexFilePrefix = vertexFilePrefix;
        this.edgeFilePrefix = edgeFilePrefix;
        this.vprogPrefix = vprogPrefix;
        this.sendMsgPrefix = sendMsgPrefix;
        this.mergeMsgPrefix = mergeMsgPrefix;
        processBuilder = new ProcessBuilder();
        this.numWorker = getNumWorker();
        this.userClass = userClass;
        this.vdClass = vdClass;
        this.edClass = edClass;
        this.msgClass = msgClass;
    }

    public void run() {
//        String[] commands =
//            {"GLOG_v=10", MPI_EXEC, "-n", numWorker.toString(), "-hostfile",
//            SPARK_CONF_WORKERS, GAE_HOME + "/build/graphx_runner", "--mm_file_prefix",
//            mmFilePrefix};
//        String [] mpiCommand = {SHELL_SCRIPT, vertexFilePrefix, edgeFilePrefix,userClass};
        String [] commands = {"/bin/bash", SHELL_SCRIPT, vertexFilePrefix, edgeFilePrefix, vprogPrefix, sendMsgPrefix, mergeMsgPrefix, userClass, clzToStr(vdClass), clzToStr(edClass), clzToStr(msgClass)};
        logger.info("Running command: " + String.join(" ", commands));
        processBuilder.command(commands);
        processBuilder.inheritIO();
        Process process = null;
        try {
            process = processBuilder.start();
            int exitCode = process.waitFor();
            logger.info("Mpi process exit code {}", exitCode);
            if (exitCode != 0) {
                throw new IllegalStateException("Error in mpi process" + exitCode);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private int getNumWorker() {
        if (!fileExists(SPARK_CONF_WORKERS)) {
            throw new IllegalStateException("SPARK_CONF_WORKERS not available");
        }
        int numLines = (int) FileUtils.getNumLinesOfFile(SPARK_CONF_WORKERS);
        if (numLines <= 0) {
            throw new IllegalStateException("empty file");
        }
        return numLines;
    }

    private String clzToStr(Class<?> clz){
        if (clz.equals(Integer.class) || clz.equals(int.class)){
            return "int";
        }
        else if(clz.equals(Long.class) || clz.equals(long.class)){
            return "long";
        }
        else if (clz.equals(Double.class) || clz.equals(double.class)){
            return "double";
        }
        throw new IllegalStateException("Unexpected clz : " + clz.getName());

    }

    private static boolean fileExists(String p) {
        return Files.exists(Paths.get(p));
    }
}

