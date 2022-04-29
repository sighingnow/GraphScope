#!/bin/bash
NUM_WORKERS=$1
shift
HOST_FILE=$1
shift
VERTEX_MAPPED_FILES=$1
shift
EDGE_MAPPED_FILES=$1
shift
VERTEX_MAPPED_SIZE=$1
shift
EDGE_MAPPED_SIZE=$1
shift
VD_TYPE=$1
shift
ED_TYPE=$1

echo "num workers:           "${NUM_WORKERS}
echo "vertex mapped files:   "${VERTEX_MAPPED_FILES}
echo "edge mapped files:     "${EDGE_MAPPED_FILES}
echo "vertex mapped size:    "${VERTEX_MAPPED_SIZE}
echo "edge mapped size:      "${EDGE_MAPPED_SIZE}
echo "vd type:               "${VD_TYPE}
echo "ed type:               "${ED_TYPE}

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/prepare.sh

cmd="GLOG_v=10 mpirun --mca btl_tcp_if_include bond0 -n ${NUM_WORKERS} -hostfile ${HOST_FILE} -x LD_PRELOAD -x GLOG_v \
-x USER_JAR_PATH -x GRAPE_JVM_OPTS ${GRAPHX_FRAGMENT_LOADER} \
--vertex_files ${VERTEX_MAPPED_FILES} --edge_files ${EDGE_MAPPED_FILES} \
--vertex_mapped_size ${VERTEX_MAPPED_SIZE} --edge_mapped_size ${EDGE_MAPPED_SIZE} \
--vd_type ${VD_TYPE} --ed_type ${ED_TYPE}"
echo "running cmd: "$cmd
eval $cmd