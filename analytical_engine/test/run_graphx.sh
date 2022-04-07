#FOO=1 BAR=2 bash -c 'echo ${FOO} ${BAR}'
#GLOG_v=20 USER_JAR_PATH=${USER_JAR_PATH} GRAPE_JVM_OPTS=${GRAPE_JVM_OPTS} ./graphx_runner   --efile '/home/graphscope/data/gstest/property/p2p-31_property_e_0#src_label=v0&dst_label=v0&label=e0' --vfile '/home/graphscope/data/gstest/property/p2p-31_property_v_0#label=v0' --user_lib_path /opt/graphscope/lib/libgrape-jni.so --user_class com.alibaba.graphscope.example.SSSP --ipc_socket /tmp/vineyard.sock.lei
source ~/gs/analytical_engine/java/grape_jvm_opts
export USER_JAR_PATH=~/gs/analytical_engine/java/graphx-on-graphscope/target/graphx-on-graphscope-0.1-shaded.jar
GLOG_v=10 mpirun -n 2 -hostfile ~/hostfile -x GLOG_v -x USER_JAR_PATH -x GRAPE_JVM_OPTS ./graphx_runner   --user_class com.alibaba.graphscope.example.SSSP --ipc_socket /tmp/vineyard.sock
