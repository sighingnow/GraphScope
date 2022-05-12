package com.alibaba.graphscope.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.graphx.impl.GrapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MPIUtils {
    private static Logger logger = LoggerFactory.getLogger(MPIUtils.class.getName());
    private static String MPI_LOG_FILE = "/tmp/graphx-mpi-log";
    private static final String GRAPHSCOPE_CODE_HOME, SPARK_HOME, GAE_HOME, SPARK_CONF_WORKERS, LAUNCH_GRAPHX_SHELL_SCRIPT, LOAD_GRAPH_SHELL_SCRIPT;
    private static final String LOAD_GRAPHX_VERTEX_MAP_SHELL_SCRIPT;
    private static final String pattern = "GlobalVertexMapID:";
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
        LAUNCH_GRAPHX_SHELL_SCRIPT = GAE_HOME + "/java/run_graphx.sh";
        if (!fileExists(LAUNCH_GRAPHX_SHELL_SCRIPT)) {
            throw new IllegalStateException("script " + LAUNCH_GRAPHX_SHELL_SCRIPT + "doesn't exist");
        }
        LOAD_GRAPH_SHELL_SCRIPT = GAE_HOME + "/java/load_graphx_fragment.sh";
        if (!fileExists(LOAD_GRAPH_SHELL_SCRIPT)) {
            throw new IllegalStateException("script " + LOAD_GRAPH_SHELL_SCRIPT + "doesn't exist");
        }
        LOAD_GRAPHX_VERTEX_MAP_SHELL_SCRIPT = GAE_HOME + "/java/load_graphx_vertex_map.sh";
        if (!fileExists(LOAD_GRAPHX_VERTEX_MAP_SHELL_SCRIPT)) {
            throw new IllegalStateException("script " + LOAD_GRAPHX_VERTEX_MAP_SHELL_SCRIPT + "doesn't exist");
        }
    }

    public static String getGAEHome(){
        return GAE_HOME;
    }

    public static <MSG,VD,ED> void launchGraphX(String fragIds, MSG initialMsg,Class<? extends MSG> msgClass,
        Class<? extends VD> vdClass, Class<? extends ED> edClass, int maxIteration,
        String vprogPath, String sendMsgPath, String mergeMsgpath, String vdataPath, long size){
        logger.info("[Driver:] {}", fragIds);
        int numWorkers = Math.min(fragIds.split(",").length, getNumWorker());
        logger.info("running mpi with {} workers", numWorkers);
//        MappedBuffer buffer = SharedMemoryRegistry.getOrCreate().mapFor(vdataPath, size);
        String[] commands = {"/bin/bash", LAUNCH_GRAPHX_SHELL_SCRIPT, String.valueOf(numWorkers),
            SPARK_CONF_WORKERS,fragIds, initialMsg.toString(), GrapeUtils.classToStr(msgClass),
            GrapeUtils.classToStr(vdClass), GrapeUtils.classToStr(edClass),
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

    private static void check(String oidType, String vidType){
        if (oidType != "int64_t" || vidType != "uint64_t"){
            throw new IllegalStateException("Not supported: " + oidType + " " + vidType);
        }
    }

    public static <MSG,VD,ED> long constructGlobalVM(String localVMIDs, String ipcSocket, String oidType, String vidType){
        check(oidType, vidType);
        logger.info("Try to construct global vm from: {}", localVMIDs);
        int numWorkers = Math.min(localVMIDs.split(",").length, getNumWorker());
        logger.info("running mpi with {} workers", numWorkers);
        String[] commands = {"/bin/bash", LOAD_GRAPHX_VERTEX_MAP_SHELL_SCRIPT, String.valueOf(numWorkers),
            SPARK_CONF_WORKERS, localVMIDs, oidType, vidType, ipcSocket};
        logger.info("Running with commands: " + String.join(" ", commands));
        long globalVMID = -1;
        long startTime = System.nanoTime();
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command(commands);
//        processBuilder.inheritIO();
        Process process = null;
        try {
            process = processBuilder.start();
            BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String str;
            while ((str = stdInput.readLine()) != null) {
                System.out.println(str);
                if (str.contains(pattern)){
                    globalVMID = Long.parseLong(str.substring(str.indexOf(pattern) + pattern.length()).trim());
                }
                //FIXME: get vm id from output.
            }
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
        logger.info("Total time spend on Loading global vertex Map : {}ms", (endTime - startTime) / 1000000);
        return globalVMID;
    }

    public static <OID, VID, GS_VD, GS_ED, GX_VD, GX_ED> String graph2Fragment(
        String[]vertexMappedFiles, String[]edgeMappedFiles, long vertexMappedSize, long edgeMappedSize, Boolean cluster, String vdType, String edType)
        throws FileNotFoundException {
        //Duplicate.
        String[] vertexMappedFilesDedup = dedup(vertexMappedFiles);
        String[] edgeMappedFilesDedup = dedup(edgeMappedFiles);
        logger.info("Before duplication, vertex files: {}", String.join( "",vertexMappedFiles));
        logger.info("After duplication, vertex files: {}",String.join("", vertexMappedFilesDedup));
        logger.info("Before duplication, edge files: {}", String.join("", edgeMappedFiles));
        logger.info("After duplication, edge files: {}",String.join("", edgeMappedFilesDedup));

        int numWorkers = 1;
        if (cluster){
            numWorkers = getNumWorker();
        }
        long startTime = System.nanoTime();
        String[] commands = {"/bin/bash", LOAD_GRAPH_SHELL_SCRIPT, String.valueOf(numWorkers), SPARK_CONF_WORKERS, String.join(":", vertexMappedFilesDedup),
            String.join(":", edgeMappedFilesDedup), String.valueOf(vertexMappedSize), String.valueOf(edgeMappedSize),
            vdType, edType};
        logger.info("Running command: " + String.join(" ", commands));
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command(commands);
        //processBuilder.inheritIO().redirectOutput(ProcessBuilder.Redirect.PIPE);
        //processBuilder.inheritIO();
        processBuilder.inheritIO().redirectOutput(new File(MPI_LOG_FILE));
        Process process = null;
        String fragIds = null;
        try {
            process = processBuilder.start();
            int exitCode = process.waitFor();
            logger.info("Mpi process exit code {}", exitCode);
            if (exitCode != 0) {
                throw new IllegalStateException("Error in mpi process" + exitCode);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        long endTime = System.nanoTime();
        logger.info("Total time spend on running mpi processes : {}ms", (endTime - startTime) / 1000000);
        //read fragids
        try {
            BufferedReader br = new BufferedReader(new FileReader(MPI_LOG_FILE));
            String line = br.readLine();
            logger.info(line);
            if (line != null){
                fragIds = line;
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }

        return fragIds;
    }

    public static String[] dedup(String[] files){
        Set<String> set = new HashSet<>(Arrays.asList(files));
        String res[] = new String[set.size()];
        res = set.toArray(res);
        return res;
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
