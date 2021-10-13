/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.grape.utils;

import com.alibaba.grape.jobConf.JobConf;

import static com.alibaba.grape.utils.JNILibraryName.JAVA_APP_JNI_LIBRARY;

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