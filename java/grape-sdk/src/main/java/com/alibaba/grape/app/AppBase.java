package com.alibaba.grape.app;

import com.alibaba.ffi.FFIVector;
import com.alibaba.grape.graph.loader.LoaderBase;
import com.alibaba.grape.graph.loader.evfileLoader.EVFileLoader;
import com.alibaba.grape.graph.loader.tableLoader.TunnelLoader;
import com.alibaba.grape.jobConf.JOB_CONF;
import com.alibaba.grape.jobConf.JobConf;
import com.alibaba.grape.utils.JobConfUtil;

import java.io.IOException;
import java.util.Map;

public interface AppBase<OID_T, VID_T, VDATA_T, EDATA_T, C
        extends ContextBase<OID_T, VID_T, VDATA_T, EDATA_T>> {
    /**
     * Load fragment from odps, store data in the passed reference params.
     *
     * @param vidBuffers
     * @param vdataBuffers
     * @param esrcBuffers
     * @param edstBuffers
     * @param edataBuffers
     * @param workerId
     * @param workerNum
     * @throws IOException
     */
    default void loadFragment(FFIVector<FFIVector<OID_T>> vidBuffers,
                              FFIVector<FFIVector<VDATA_T>> vdataBuffers,
                              FFIVector<FFIVector<OID_T>> esrcBuffers,
                              FFIVector<FFIVector<OID_T>> edstBuffers,
                              FFIVector<FFIVector<EDATA_T>> edataBuffers, int workerId,
                              int workerNum) throws IOException {
        // JobConf job = createGraphJob();
        JobConf job = JobConfUtil.readJobConf(JOB_CONF.DEFAULT_JOB_CONF_PATH);
        LoaderBase<OID_T, VDATA_T, EDATA_T> loader;

        Map<String, String> env = System.getenv();
        String inputType = env.getOrDefault("input_type", "null");
        if (inputType.equals("null")) {
            System.err.println("Pls set input type before loading graph");
            return;
        } else if (inputType.equals("odps")) {
            loader = new TunnelLoader<OID_T, VDATA_T, EDATA_T>(workerId, workerNum, job);
        } else if (inputType.equals("evfile")) {
            loader = new EVFileLoader<OID_T, VDATA_T, EDATA_T>(workerId, workerNum, job);
        } else {
            System.err.println("Unexpected env var " + inputType);
            return;
        }
        loader.loadFragment(vidBuffers, vdataBuffers, esrcBuffers, edstBuffers, edataBuffers);
        // System.out.println("after java load...");
        System.gc();
    }
}
