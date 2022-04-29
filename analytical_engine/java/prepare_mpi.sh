#!/bin/bash

export LD_PRELOAD=$LD_PRELOAD:/usr/local/lib64/libssl.so.1.1
DEFAULT_SPARK_HOME=~/spark/spark-3.2.1-bin-hadoop2.7

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
echo "Script dir:"${SCRIPT_DIR}
export GRAPHSCOPE_CODE_HOME=${SCRIPT_DIR}/../../
echo "GS_CODEHOME: "${GRAPHSCOPE_CODE_HOME}

source  ${SCRIPT_DIR}/grape_jvm_opts
export USER_JAR_PATH=${SCRIPT_DIR}/graphx-on-graphscope/target/graphx-on-graphscope-0.1-shaded.jar
GRAPHX_RUNNER=${SCRIPT_DIR}/../build/graphx_runner
GRAPHX_FRAGMENT_LOADER=${SCRIPT_DIR}/../graphx_fragment_loader

if [ -f "${USER_JAR_PATH}" ]; then
    echo "user jar exists."
else
    echo "user jar doesn't exist"
    exit 1;
fi

if [ -f "${GRAPHX_RUNNER}" ]; then
    echo "runner exists."
else
    echo "runner doesn't exist"
    exit 1;
fi

if [ -z "${SPARK_HOME}" ];then
    echo "using default spark home "${DEFAULT_SPARK_HOME}
    export SPARK_HOME=${DEFAULT_SPARK_HOME}
fi

SPARK_CONF_WORKER=${SPARK_HOME}/conf/workers
echo "conf workers: "${SPARK_CONF_WORKER}