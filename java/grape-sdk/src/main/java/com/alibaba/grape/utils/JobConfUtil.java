package com.alibaba.grape.utils;

import com.alibaba.grape.jobConf.JobConf;

import static com.alibaba.grape.utils.CPP_LIBRARY_STRINGS.JAVA_APP_JNI_LIBRARY;

public class JobConfUtil {
    static {
        try {
            System.loadLibrary(JAVA_APP_JNI_LIBRARY);
        } catch (UnsatisfiedLinkError e) {
            System.out.println("load " + JAVA_APP_JNI_LIBRARY + " failed");
        }
    }

    public static JobConf readJobConf(String path) {
        JobConf graphJob = new JobConf();

        // String path = "/tmp/" + pid + ".job_conf";
        // String path = "/tmp/pie-sdk.job_conf";
        // System.out.println("create graph conf from" + path);
        try {
            // DataInputStream dis = new DataInputStream(new FileInputStream(path));
            graphJob.readConfig(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return graphJob;
    }
}