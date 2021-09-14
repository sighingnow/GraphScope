# find vineyard.
# analytical_engine HOME, find the HOME using relative path.
ENGINE_HOME="$(
  cd "$(dirname "$0")/.." >/dev/null 2>&1
  pwd -P
)"

export VINEYARD_HOME=/usr/local/bin
socket_file=/tmp/vineyard.sock
test_dir=${ENGINE_HOME}/../gstest/

function start_vineyard() {
  pushd "${ENGINE_HOME}/build"
  pkill vineyardd || true
  pkill etcd || true
  echo "[INFO] vineyardd will using the socket_file on ${socket_file}"

  timestamp=$(date +%Y-%m-%d_%H-%M-%S)
  vineyardd \
    --socket ${socket_file} \
    --size 2000000000 \
    --etcd_prefix "${timestamp}" \
    --etcd_endpoint=http://127.0.0.1:3457 &
  set +m
  sleep 5
  info "vineyardd started."
  popd
}

########################################################
# Run apps over property graphs on vineyard.
# Arguments:
#   - num_of_process.
#   - executable.
#   - rest args.
########################################################
function run_vy() {
  num_procs=$1
  shift
  executable=$1
  shift
  socket_file=$1
  shift
  e_label_num=$1
  shift
  e_prefix=$1
  shift
  v_label_num=$1
  shift
  v_prefix=$1
  shift

  cmd="${cmd_prefix} -n ${num_procs} --host localhost:${num_procs} ${executable} ${socket_file}"

  cmd="${cmd} ${e_label_num}"
  for ((e=0;e<e_label_num;++e))
  do
    cmd="${cmd} '"
    first=true
    for ((src=0;src<v_label_num;src++))
    do
      for ((dst=0;dst<v_label_num;++dst))
      do
	if [ "$first" = true ]
        then
          first=false
          cmd="${cmd}${e_prefix}_${src}_${dst}_${e}#src_label=v${src}&dst_label=v${dst}&label=e${e}"
        else
          cmd="${cmd};${e_prefix}_${src}_${dst}_${e}#src_label=v${src}&dst_label=v${dst}&label=e${e}"
	fi
      done
    done
    cmd="${cmd}'"
  done

  cmd="${cmd} ${v_label_num}"
  for ((i = 0; i < v_label_num; i++)); do
    cmd="${cmd} ${v_prefix}_${i}#label=v${i}"
  done

  cmd="${cmd} $*"

  echo "${cmd}"
  eval "${cmd}"
  echo "Finished running app ${executable} with vineyard."
}

cmd_prefix="mpirun"

pushd "${ENGINE_HOME}"/build

#start_vineyard

demo_jar=/home/admin/.m2/repository/com/alibaba/grape/graphscope-demo/0.1/graphscope-demo-0.1-jar-with-dependencies.jar
GRAPE_SDK_BUILD=/home/admin/GAE-ODPSGraph/pie-sdk/grape-sdk/target/classes/
GRAPE_SDK_BUILD_NATIVE=${GRAPE_SDK_BUILD}/natives/linux_64
VINEYARD_GRAPH_BUILD=/home/admin/GAE-ODPSGraph/pie-sdk/vineyard-graph/target/classes/
VINEYARD_GRAPH_BUILD_NATIVE=${VINEYARD_GRAPH_BUILD}/natives/linux_64

#export RUN_CP=${RUN_CP}:${DIR}/../../../GAE-ODPSGraph/pie-sdk/grape-sdk/target/classes
# put sdk before demo due to the version of guava, 15.0 vs 30-jre
export RUN_CP=${RUN_CP}:~/.m2/repository/com/google/guava/guava/30.1.1-jre/guava-30.1.1-jre.jar
export RUN_CP=${GRAPE_SDK_BUILD}:${VINEYARD_GRAPH_BUILD}
export RUN_CP=${RUN_CP}:${demo_jar}
export RUN_CP=${RUN_CP}:~/.m2/repository/com/alibaba/ffi/llvm4jni-runtime/0.1/llvm4jni-runtime-0.1-jar-with-dependencies.jar
echo "run class path "${RUN_CP}
echo "java libraray path "${GAE_DIR}/build:${DIR}/build:${GRAPE_LITE_JNI_SO_PATH}
export RUN_JVM_OPTS="-Djava.library.path=${GRAPE_SDK_BUILD_NATIVE}:${VINEYARD_GRAPH_BUILD_NATIVE}:/usr/local/lib -Djava.class.path=${RUN_CP}"
#-verbose:class 
np=1
GLOG_v=1 run_vy ${np} ./run_java_property_app "${socket_file}" 2 "${test_dir}"/new_property/v2_e2/twitter_e 2 "${test_dir}"/new_property/v2_e2/twitter_v 0 1 io.graphscope.example.sssp.SSSPDefault
