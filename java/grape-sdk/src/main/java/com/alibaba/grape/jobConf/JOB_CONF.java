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

package com.alibaba.grape.jobConf;

/**
 * 定义 PIE sdk 作业配置中可以允许用户自定义的配置参数.
 */
public class JOB_CONF {
    public final static String OUTPUT_PATH = "outputPath";
    public final static String MAX_ITERATION = "maxIteration";
    public final static String DIRECTED = "directed";

    public static final String INPUT_TABLES = "inputTables";
    public static final String OUTPUT_TABLES = "outputTables";

    public static final String OID_TYPE = "oidType";
    public static final String VID_TYPE = "vidType";
    public static final String VERTEX_DATA_TYPE = "vertexDataType";
    public static final String EDGE_DATA_TYPE = "edgeDataType";
    public static final String MESSAGE_TYPES = "messageTypes";
    public static final String APP_CLASS = "appClass";
    public static final String APP_CONTEXT_CLASS = "contextClass";

    public static final String EDGE_FILE_PATH = "edgeFilePath";
    public static final String VERTEX_FILE_PATH = "vertexFilePath";
    public static final String EV_FILE_LINE_PARSER = "evFileLineParser";

    public static final String DEFAULT_JOB_CONF_PATH = "/tmp/gs/java_pie.conf";
}
