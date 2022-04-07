#!/bin/bash


#vertex mm file
V_FILE_PREFIX=$1
shift
E_FILE_PREFIX=$1
shift
USER_CLASS=$1

echo "User class:    "${V_FILE_PREFIX}
echo "User class:    "${E_FILE_PREFIX}
echo "User class:    "${USER_CLASS}



DEFAULT_SPARK_HOME=~/spark/spark-3.2.1-bin-hadoop2.7

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

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
    SPARK_CONF_WORKER=${SPARK_HOME}/conf/workers
    echo "conf workers: "${SPARK_CONF_WORKER}
fi


cmd="GLOG_v=10 mpirun -n 2 -hostfile ${SPARK_CONF_WORKER} -x GLOG_v -x USER_JAR_PATH -x GRAPE_JVM_OPTS ${GRAPHX_RUNNER} --user_class ${USER_CLASS} --vertex_mm_file_prefix ${V_FILE_PREFIX} --edge_mm_file_prefix ${E_FILE_PREFIX}"
echo "running cmd: "$cmd
eval $cmd