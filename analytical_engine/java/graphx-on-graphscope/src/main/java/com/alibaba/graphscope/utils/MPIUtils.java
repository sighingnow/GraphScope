package com.alibaba.graphscope.utils;

import com.alibaba.graphscope.graphx.SharedMemoryRegistry;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.graphx.impl.GrapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MPIUtils {
    private static Logger logger = LoggerFactory.getLogger(MPIUtils.class.getName());
    private static final String GRAPHSCOPE_CODE_HOME, SPARK_HOME, GAE_HOME, SPARK_CONF_WORKERS,SHELL_SCRIPT;
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
    public static String getGAEHome(){
        return GAE_HOME;
    }

    public static <MSG> void launchGraphX(String fragIds, MSG initialMsg,Class<? extends MSG> msgClass, int maxIteration, String vprogPath, String sendMsgPath, String mergeMsgpath, String vdataPath, long size){
        int numWorkers = Math.min(fragIds.split(",").length, getNumWorker());
        logger.info("running mpi with {} workers", numWorkers);
//        MappedBuffer buffer = SharedMemoryRegistry.getOrCreate().mapFor(vdataPath, size);
        String[] commands = {"/bin/bash", SHELL_SCRIPT, String.valueOf(numWorkers), fragIds, initialMsg.toString(), GrapeUtils.classToStr(msgClass),
            String.valueOf(maxIteration), vprogPath, sendMsgPath, mergeMsgpath,vdataPath,
            String.valueOf(size)};
        logger.info("Running with commands: " + String.join(" ", commands));
        long startTime = System.nanoTime();
        ProcessBuilder processBuilder = new ProcessBuilder();
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
        long endTime = System.nanoTime();
        logger.info("Total time spend on running mpi processes : {}ms", (endTime - startTime) / 1000000);
    }

    private static boolean fileExists(String p) {
        return Files.exists(Paths.get(p));
    }

    public static int getNumWorker() {
        if (!fileExists(SPARK_CONF_WORKERS)) {
            throw new IllegalStateException("SPARK_CONF_WORKERS not available");
        }
        int numLines = (int) FileUtils.getNumLinesOfFile(SPARK_CONF_WORKERS);
        if (numLines <= 0) {
            throw new IllegalStateException("empty file");
        }
        return numLines;
    }
    public static List<String> getWorkers() throws IOException {
        if (!fileExists(SPARK_CONF_WORKERS)) {
            throw new IllegalStateException("SPARK_CONF_WORKERS not available");
        }
        BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(SPARK_CONF_WORKERS)));
        List<String> res = new ArrayList<>();
        String line = null;
        while ((line = bufferedReader.readLine()) != null){
            res.add(line);
        }
        return res;
    }

    /**
     * Distribute these files to all worker nodes.
     * @param files file names.
     */
    public static void distribute(String... files) throws IOException {
        List<String> hostNames = getWorkers();
        for (String host: hostNames){
            FileUtils.sendFiles(files, host);
        }
    }
}
