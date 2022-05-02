#!/bin/bash

NUM_WORKERS=$1
shift
HOST_FILE=$1
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

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/prepare_mpi.sh

# 1.first distribute serialized vprog functions.
for host_line in `cat ${SPARK_CONF_WORKER} | awk '{ print $1; }'`;
do
    echo ${host}
    scp ${VPROG_SERIALIZATION} ${host}:${VPROG_SERIALIZATION}
    scp ${SEND_MSG_SERIALIZATION} ${host}:${SEND_MSG_SERIALIZATION}
    scp ${MERGE_MSG_SERIALIZATION} ${host}:${MERGE_MSG_SERIALIZATION}
done

#cmd="GLOG_v=10 mpirun -n 1 -hostfile ${SPARK_CONF_WORKER} -x GLOG_v -x GRAPHSCOPE_CODE_HOME -x USER_JAR_PATH -x GRAPE_JVM_OPTS ${GRAPHX_RUNNER} --user_class ${USER_CLASS} --vertex_mm_file_prefix ${V_FILE_PREFIX} --edge_mm_file_prefix ${E_FILE_PREFIX}"
cmd="GLOG_v=10 mpirun --mca btl_tcp_if_include bond0 -n ${NUM_WORKERS} -hostfile ${HOST_FILE} -x LD_PRELOAD -x GLOG_v \
-x USER_JAR_PATH -x GRAPE_JVM_OPTS ${GRAPHX_RUNNER} \
--vprog_path ${VPROG_SERIALIZATION} --send_msg_path ${SEND_MSG_SERIALIZATION} \
--merge_msg_path ${MERGE_MSG_SERIALIZATION} --msg_class ${MSG_CLASS} \
--vd_class ${VD_CLASS} --ed_class ${ED_CLASS} \
--initial_msg ${INIT_MSG} --vdata_path ${VDATA_PATH} --vdata_size ${VDATA_SIZE} \
 --max_iterations ${MAX_ITERATION} --frag_ids ${FRAG_IDS}"
echo "running cmd: "$cmd
eval $cmd
