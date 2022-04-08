#!/bin/bash

echo $1$2$3
#vertex mm file
V_FILE_PREFIX=$1
shift
E_FILE_PREFIX=$1
shift
USER_CLASS=$1

echo "vfile prefix:    "${V_FILE_PREFIX}
echo "efile preifx:    "${E_FILE_PREFIX}
echo "User class:    "${USER_CLASS}



DEFAULT_SPARK_HOME=~/spark/spark-3.2.1-bin-hadoop2.7

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
echo "Script dir:"${SCRIPT_DIR}
export GRAPHSCOPE_CODE_HOME=${SCRIPT_DIR}/../../
echo "GS_CODEHOME: "${GRAPHSCOPE_CODE_HOME}

source  ${SCRIPT_DIR}/grape_jvm_opts
export USER_JAR_PATH=${SCRIPT_DIR}/graphx-on-graphscope/target/graphx-on-graphscope-0.1-shaded.jar
GRAPHX_RUNNER=${SCRIPT_DIR}/../build/graphx_runner

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

cmd="GLOG_v=10 mpirun -n 1 -hostfile ${SPARK_CONF_WORKER} -x GLOG_v -x GRAPHSCOPE_CODE_HOME -x USER_JAR_PATH -x GRAPE_JVM_OPTS ${GRAPHX_RUNNER} --user_class ${USER_CLASS} --vertex_mm_file_prefix ${V_FILE_PREFIX} --edge_mm_file_prefix ${E_FILE_PREFIX}"
echo "running cmd: "$cmd
eval $cmd
