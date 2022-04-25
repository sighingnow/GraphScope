#!/bin/bash

NUM_WORKERS=$1
shift
FRAG_IDS=$1
shift
INIT_MSG=$1
shift
MSG_CLASS=$1
shift
VD_CLASS=$1
shift
ED_CLASS=$1
shift
MAX_ITERATION=$1
shift
VPROG_SERIALIZATION=$1
shift
SEND_MSG_SERIALIZATION=$1
shift
MERGE_MSG_SERIALIZATION=$1
shift
VDATA_PATH=$1
shift
VDATA_SIZE=$1

echo "vprog               "${VPROG_SERIALIZATION}
echo "send_msg            "${SEND_MSG_SERIALIZATION}
echo "merge msg           "${MERGE_MSG_SERIALIZATION}
echo "msgClass            "${MSG_CLASS}
echo "vdClass             "${VD_CLASS}
echo "edClass             "${ED_CLASS}
echo "initial msg         "${INIT_MSG}
echo "vdata map size      "${VDATA_SIZE}
echo "frag ids            "${FRAG_IDS}
echo "num workers:        "${NUM_WORKERS}

export LD_PRELOAD=$LD_PRELOAD://usr/local/lib64/libssl.so.1.1
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

# 1.first distribute serialized vprog functions.
for host in `cat ${SPARK_CONF_WORKER}`;
do
    echo ${host}
    scp ${VPROG_SERIALIZATION} ${host}:${VPROG_SERIALIZATION}
    scp ${SEND_MSG_SERIALIZATION} ${host}:${SEND_MSG_SERIALIZATION}
    scp ${MERGE_MSG_SERIALIZATION} ${host}:${MERGE_MSG_SERIALIZATION}
done

#cmd="GLOG_v=10 mpirun -n 1 -hostfile ${SPARK_CONF_WORKER} -x GLOG_v -x GRAPHSCOPE_CODE_HOME -x USER_JAR_PATH -x GRAPE_JVM_OPTS ${GRAPHX_RUNNER} --user_class ${USER_CLASS} --vertex_mm_file_prefix ${V_FILE_PREFIX} --edge_mm_file_prefix ${E_FILE_PREFIX}"
cmd="GLOG_v=10 mpirun --mca btl_tcp_if_include bond0 -n ${NUM_WORKERS} -host d50 -x LD_PRELOAD -x GLOG_v \
-x GRAPHSCOPE_CODE_HOME -x USER_JAR_PATH -x GRAPE_JVM_OPTS ${GRAPHX_RUNNER} \
--vprog_path ${VPROG_SERIALIZATION} --send_msg_path ${SEND_MSG_SERIALIZATION} \
--merge_msg_path ${MERGE_MSG_SERIALIZATION} --msg_class ${MSG_CLASS} \
--vd_class ${VD_CLASS} --ed_class ${ED_CLASS} \
--initial_msg ${INIT_MSG} --vdata_path ${VDATA_PATH} --vdata_size ${VDATA_SIZE} \
 --max_iterations ${MAX_ITERATION} --frag_ids ${FRAG_IDS}"
echo "running cmd: "$cmd
eval $cmd
