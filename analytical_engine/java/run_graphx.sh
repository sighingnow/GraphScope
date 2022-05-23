#!/bin/bash

NUM_WORKERS=$1
shift
HOST_SLOT=$1
shift
VD_CLASS=$1
shift
ED_CLASS=$1
shift
MSG_CLASS=$1
shift
VM_IDS=$1
shift
CSR_IDS=$1
shift
VDATA_IDS=$1
shift
SERIAL_PATH=$1
shift
MAX_ITERATION=$1

echo "serial path         "${SERIAL_PATH}
echo "num workers:        "${NUM_WORKERS}
echo "host file           "${HOST_SLOT}
echo "vm ids:             "${VM_IDS}
echo "csr ids:            "${CSR_IDS}
echo "vdata ids:          "${VDATA_IDS}
echo "max iter            "${MAX_ITERATION}

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/prepare_mpi.sh

# 1.first distribute serialized vprog functions.
for host in `cat ${SPARK_CONF_WORKER} | awk '{ print $1; }'`;
do
    echo ${host}
    scp ${SERIAL_PATH} ${host}:${SERIAL_PATH}
done

#cmd="GLOG_v=10 mpirun -n 1 -hostfile ${SPARK_CONF_WORKER} -x GLOG_v -x GRAPHSCOPE_CODE_HOME -x USER_JAR_PATH -x GRAPE_JVM_OPTS ${GRAPHX_RUNNER} --user_class ${USER_CLASS} --vertex_mm_file_prefix ${V_FILE_PREFIX} --edge_mm_file_prefix ${E_FILE_PREFIX}"
cmd="GLOG_v=10 mpirun --mca btl_tcp_if_include bond0 -n ${NUM_WORKERS} -host ${HOST_SLOT} -x LD_PRELOAD -x GLOG_v \
-x USER_JAR_PATH -x GRAPE_JVM_OPTS ${GRAPHX_RUNNER} \
--vd_class ${VD_CLASS} --ed_class ${ED_CLASS} --msg_class ${MSG_CLASS} \
--serial_path ${SERIAL_PATH} --vm_ids ${VM_IDS} --csr_ids ${CSR_IDS} --vdata_ids ${VDATA_IDS} \
--max_iterations ${MAX_ITERATION}"
echo "running cmd: "$cmd >&2
eval $cmd
