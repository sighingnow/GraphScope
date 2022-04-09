#!/bin/bash

echo $1$2$3
#vertex mm file
V_FILE_PREFIX=$1
shift
E_FILE_PREFIX=$1
shift
VPROG_SERIALIZATION=$1
shift
SEND_MSG_SERIALIZATION=$1
shift
MERGE_MSG_SERIALIZATION=$1
shift
USER_CLASS=$1
shift
vdClass=$1
shift
edClass=$1
shift
msgClass=$1
shift
initialMsg=$1
shift
max_partition_id=$1
shift
mapped_size=$1

echo "vfile prefix:    "${V_FILE_PREFIX}
echo "efile preifx:    "${E_FILE_PREFIX}
echo "user class:      "${USER_CLASS}
echo "vprog            "${VPROG_SERIALIZATION}
echo "send_msg         "${SEND_MSG_SERIALIZATION}
echo "merge msg        "${MERGE_MSG_SERIALIZATION}
echo "vd class         "${vdClass}
echo "edClass          "${edClass}
echo "msgClass         "${msgClass}
echo "initial msg      "${initialMsg}
echo "max partition_id "${max_partition_id}
echo "mapped isze      "${mapped_size}


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

#cmd="GLOG_v=10 mpirun -n 1 -hostfile ${SPARK_CONF_WORKER} -x GLOG_v -x GRAPHSCOPE_CODE_HOME -x USER_JAR_PATH -x GRAPE_JVM_OPTS ${GRAPHX_RUNNER} --user_class ${USER_CLASS} --vertex_mm_file_prefix ${V_FILE_PREFIX} --edge_mm_file_prefix ${E_FILE_PREFIX}"
cmd="GLOG_v=10 mpirun -n 1 -host s3 -x GLOG_v -x GRAPHSCOPE_CODE_HOME -x USER_JAR_PATH -x GRAPE_JVM_OPTS ${GRAPHX_RUNNER} --user_class ${USER_CLASS} --vertex_mm_file_prefix ${V_FILE_PREFIX} --edge_mm_file_prefix ${E_FILE_PREFIX} --vprog_serialization ${VPROG_SERIALIZATION} --send_msg_serialization ${SEND_MSG_SERIALIZATION} --merge_msg_serialization ${MERGE_MSG_SERIALIZATION} --vd_class ${vdClass} --ed_class ${edClass} --msg_class ${msgClass} --initial_msg ${initialMsg} --max_partition_id ${max_partition_id} --mapped_size ${mapped_size}"
echo "running cmd: "$cmd
eval $cmd
