#!/bin/bash
NUM_WORKERS=$1
shift
HOST_FILE=$1
shift
LOCAL_VM_IDS=$1
shift
OID_T=$1
shift
VID_T=$1
shift
IPC_SOCKET=$1
shift

echo "num workers:            "${NUM_WORKERS}
echo "oid type:               "${OID_T}
echo "vid type:               "${VID_T}
echo "host file:              "${HOST_FILE}
echo "local vm ids            "${LOCAL_VM_IDS}

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
source ${SCRIPT_DIR}/prepare_mpi.sh

cmd="GLOG_v=10 mpirun --mca btl_tcp_if_include bond0 -n ${NUM_WORKERS} --hostfile ${HOST_FILE} -x LD_PRELOAD -x GLOG_v \
${GRAPHX_GLOBAL_VM_LOADER} --oid_type ${OID_T} --vid_type ${VID_T} --local_vm_ids ${LOCAL_VM_IDS} --ipc_socket ${IPC_SOCKET}"
echo "running cmd: "$cmd
eval $cmd
