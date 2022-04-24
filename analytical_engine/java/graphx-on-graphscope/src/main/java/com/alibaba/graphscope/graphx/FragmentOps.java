package com.alibaba.graphscope.graphx;

import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.utils.MPIUtils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.spark.graphx.impl.GrapeGraphImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines static method which enabling convertion between graphx.GrapeGraph and fragment.
 */
public class FragmentOps {
    private static Logger logger = LoggerFactory.getLogger(FragmentOps.class.getName());
    private static  String GRAPHX_LOADER = "graphx_fragment_loader";
    private static String MPI_EXEC = "mpirun";
    private static String VERTEX_MAPPED_FILES = "--vertex_mapped_files";
    private static String EDGE_MAPPED_FILES = "--edge_mapped_files";
    private static String VERTEX_MAPPED_SIZE = "--vertex_mapped_size";
    private static String EDGE_MAPPED_SIZE = "--edge_mapped_size";


    public static <OID, VID, GS_VD, GS_ED, GX_VD, GX_ED> String graph2Fragment(
        String[]vertexMappedFiles, String[]edgeMappedFiles, long vertexMappedSize, long edgeMappedSize, Boolean cluster) {
        //Duplicate.
        String[] vertexMappedFilesDedup = dedup(vertexMappedFiles);
        String[] edgeMappedFilesDedup = dedup(edgeMappedFiles);
        logger.info("Before duplication, vertex files: {}", String.join(":", vertexMappedFiles));
        logger.info("After duplication, vertex files: {}",String.join(":", vertexMappedFilesDedup));
        logger.info("Before duplication, edge files: {}", String.join(":", edgeMappedFiles));
        logger.info("After duplication, edge files: {}",String.join(":", edgeMappedFilesDedup));

        int numWorkers = 1;
        if (cluster){
            numWorkers = MPIUtils.getNumWorker();
        }
        long startTime = System.nanoTime();
        String[] commands = {MPI_EXEC, "-n", String.valueOf(numWorkers), VERTEX_MAPPED_FILES , String.join(":", vertexMappedFilesDedup),
            EDGE_MAPPED_FILES,  String.join(":", edgeMappedFilesDedup), VERTEX_MAPPED_SIZE,
            String.valueOf(vertexMappedSize), EDGE_MAPPED_SIZE, String.valueOf(edgeMappedSize)};
        logger.info("Running command: " + String.join(" ", commands));
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command(commands);
//        processBuilder.inheritIO();
        Process process = null;
        String fragIds = null;
        try {
            process = processBuilder.start();
            BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = br.readLine();
            while (line != null){
                logger.info(line);
                if (line.contains("[FragIds]:")){
                    int pos = line.indexOf("[FragIds]:");
                    fragIds = line.substring(pos + 10);
                }
                line = br.readLine();
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
        logger.info("Total time spend on running mpi processes : {}ms", (endTime - startTime) / 1000000);
        return fragIds;
    }

    public static String[] dedup(String[] files){
        Set<String> set = new HashSet<>(Arrays.asList(files));
        String[] res = (String[]) set.toArray();
        return res;
    }

    public static <OID, VID, GS_VD, GS_ED, GX_VD, GX_ED> GrapeGraphImpl<GX_VD, GX_ED> fragment2Graph(
        ArrowProjectedFragment<OID, VID, GS_VD, GS_ED> fragment) {
        return null;
    }
}
