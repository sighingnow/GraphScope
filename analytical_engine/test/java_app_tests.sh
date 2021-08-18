#!/bin/bash
#echo "when running this scripts, make sure your grape-sdk is compiled and installed"

set -e
usage(){
	echo "#--------------------------------------------------------------"
	echo "#            run app."
	echo "#    -p: specify the process number"
	echo "#    -s: specify the serialization opt"
	echo "#         serialize"
	echo "# 	    deserialize"
	echo "#		    noserialize"
	echo "#	   -m: thread num"
	echo "#    -d: which dataset"
	echo "#		   possible candidates"
	echo "# 		   - twitter"
	echo "#			   - p2p"
	echo "#	   -c: whether to preprocess(codegen for user jar)"
	echo "#    -a: specify the name of the app you want to run, e.g. sssp"
    echo "#        possible candidates: "
   	echo "#            - sssp-default : the default sequential sssp"
    echo "#            - sssp-mirror : default sequential sssp wit ffi mirror"
    echo "#            - others"
    echo "#    -t specifying the directory where test data lies"
	echo "#    -h: show the usage"
	echo "#--------------------------------------------------------------"
	exit
}
DIR=$(pushd $(dirname $BASH_SOURCE[0]) > /dev/null && pwd && popd > /dev/null)
GAE_DIR=${DIR}/../
M2_REPO_GRAPE=~/.m2/repository/com/alibaba/grape

export input_type=evfile
export grape_demo_jar=${M2_REPO_GRAPE}/grape-demo/0.1/grape-demo-0.1-jar-with-dependencies.jar
export grape_sdk_jar=${M2_REPO_GRAPE}/grape-sdk/0.1/grape-sdk-0.1-jar-with-dependencies.jar
export grape_processor_jar=${M2_REPO_GRAPE}/grape-processor/0.1/grape-processor-0.1-jar-with-dependencies.jar

# -XX:+PrintInlining -XX:+PrintCompilation
#export RUN_JVM_OPTS=${RUN_JVM_OPTS}" -XX:+PrintInlining -XX:+PrintCompilation"
export num_worker=1
export task_main_class=
export app_class=
export app_context_class=
export app_type=
export preprocess_flag=false
serialize_opt=noserialize
prefix=default

#some app related params
export maxiter=100
export source_oid=1
export thread_num=1
while getopts "s:p:m:a:d:t:ch" opt;
do
    case $opt in
    t)
	   echo "setting test direction"
           export dataset_dir=${OPTARG}
	   ;;
	s)
	    echo "serialize option"${OPTARG}
	    if [ "${OPTARG}"x = "serialize"x ]
	    then
		    export serialize_opt=serialize
	    elif [ "${OPTARG}"x = "deserialize"x ]
	    then 
		    export serialize_opt=deserialize
	    elif [ "${OPTARG}"x = "noserialize"x ]
	    then
		    export serialize_opt=noSerialize
		    prefix=
	    else
		    echo "unrecoginized serialize opt"${OPTARG}
		    exit;
	    fi
	    ;;
    p)
        echo "setting num of workers to"${OPTARG}
        export num_worker=${OPTARG}
        ;;
	m)
	    echo "num threads"${OPTARG}
	    export thread_num=${OPTARG}
	    ;;
    a)
        echo "-------------------------------------------------"
        echo "seting applications"
        if [ "${OPTARG}"x = "sssp-default"x ]
        then
            export app_type=${OPTARG}
            export task_main_class=com.alibaba.grape.SSSPMainClass
            export app_class=com.alibaba.grape.sample.sssp.SSSPDefault
            export app_context_class=com.alibaba.grape.sample.sssp.SSSPDefaultContext     
         elif [ "${OPTARG}"x = "sssp-parallel"x ]
	 then
            export app_type=${OPTARG}
            export task_main_class=com.alibaba.grape.SSSPMainClass
            export app_class=com.alibaba.grape.sample.sssp.SSSPParallel
            export app_context_class=com.alibaba.grape.sample.sssp.SSSPParallelContext
        elif [ "${OPTARG}"x = "traverse"x ]
        then
            export app_type=${OPTARG}
            export task_main_class=com.alibaba.grape.TraverseMainClass
            #export app_class=com.alibaba.grape.sample.traverse.Traverse4
            export app_class=com.alibaba.grape.sample.traverse.TraverseAdjListv3
            export app_context_class=com.alibaba.grape.sample.traverse.Traverse4Context
        else 
            echo "unrecongized application "${OPTARG}
            exit;
        fi
        echo "-------------------------------------------------"
        echo "task main class: "${task_main_class}
        echo "app class "${app_class}
        echo "app context class "${app_context_class}
        echo "-------------------------------------------------"
        if [ "${app_class}"x = ""x ]
        then
            echo "app class empty"
            exit;
        elif [ "${task_main_class}"x = ""x ]
        then
            echo "task main class empty"
            exit;
        elif [ "${app_context_class}"x = ""x ]
        then
            echo "app context class empty"
            exit;
        else 
            echo "ok! with app class and task main class"
        fi
        ;;
    d)
        echo "-------------------------------------------------"
        echo "seting dataset"
        if [ "${OPTARG}"x = "twitter"x ]
        then 
            export vfile_path=${dataset_dir}"/twitter.v"
            export efile_path=${dataset_dir}"/twitter.e"
            export source_oid=1
            export prefix=twitter 
        elif [ "${OPTARG}"x = "p2p"x ]
        then
            export vfile_path=${dataset_dir}"/p2p-31.v"
            export efile_path=${dataset_dir}"/p2p-31.e"
            export prefix=p2p 
            export source_oid=6
        else 
            echo "unrecoginzed dataset "${OPTARG}
            exit;
        fi
        echo "-------------------------------------------------"
        echo "vfile path"${vfile_path}
        echo "efile path"${efile_path}
        echo "-------------------------------------------------"
        ;;
    c)
        export preprocess_flag=true
        ;;
    ?)
        echo "Invalid option: -$OPTARG" 
        usage
        ;;
    esac
done

########################################################
# Proceprocess for the input jars
# Arguments:
#   None
########################################################
if [ "$preprocess_flag" = true ];
then
    echo "-------------------------------------------------"
    echo "preprocess"
    echo "remove the files generated last times..."
    if [[ -d ${DIR}/grape-ffi ]];
    then
        rm -rf ${DIR}/grape-ffi
    fi
    mkdir ${DIR}/grape-ffi
    echo "finish removing ffi, now run pie preprocess..."
    export PRE_CP=${grape_demo_jar}:${grape_sdk_jar}:${grape_processor_jar}
    export JVM_OPTS="-Djava.library.path=${GAE_DIR}/build: -Djava.class.path=${PRE_CP}"
    ${DIR}/build/run_java_app_preprocess \
                        ${task_main_class} \
                        ${grape_demo_jar} \
                        /tmp/demo.properties ${vfile_path} ${efile_path}
    cp -r `cat codegen_path`/* ${DIR}/grape-ffi
fi
export GRAPE_GEN_PATH=${DIR}/grape-ffi

# compile the generated cpp files into one dynamic library.
echo "-------------------------------------------------"
echo "start compiling"
if [[ -d ${DIR}/build ]];
then
pushd ${DIR}/build
../clang-cmake.sh .. 
make -j    
fi
popd
echo "-----------------------------------------"
echo "----------------compilation ok!----------"


########################################################
# Run the queries
# Arguments:
#   None
########################################################
GRAPE_LITE_JNI_SO_PATH=~/GAE-ODPSGraph/pie-sdk/grape-sdk/target/classes/
export RUN_CP=${DIR}/grape-ffi/CLASS_OUTPUT
#export RUN_CP=${RUN_CP}:${DIR}/../../../GAE-ODPSGraph/pie-sdk/grape-sdk/target/classes
# put sdk before demo due to the version of guava, 15.0 vs 30-jre
export RUN_CP=${RUN_CP}:~/.m2/repository/com/google/guava/guava/30.1.1-jre/guava-30.1.1-jre.jar
export RUN_CP=${RUN_CP}:~/.m2/repository/com/alibaba/ffi/ffi/0.1/ffi-0.1.jar
export RUN_CP=${RUN_CP}:${grape_sdk_jar}:${grape_demo_jar}
export RUN_CP=${RUN_CP}:~/.m2/repository/com/alibaba/ffi/llvm4jni-runtime/0.1/llvm4jni-runtime-0.1-jar-with-dependencies.jar
echo "run class path "${RUN_CP}
echo "java libraray path "${GAE_DIR}/build:${DIR}/build:${GRAPE_LITE_JNI_SO_PATH}
export RUN_JVM_OPTS="-Djava.library.path=${GAE_DIR}/build:${DIR}/build:${GRAPE_LITE_JNI_SO_PATH}:/usr/local/lib:/home/admin/alibaba-ffi/llvm/target/classes -Djava.class.path=${RUN_CP} -Dcom.alibaba.ffi.rvBuffer=2147483648 -XX:+StartAttachListener -XX:+PreserveFramePointer -XX:+UseParallelGC -XX:+UseParallelOldGC -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UnlockDiagnosticVMOptions -XX:LoopUnrollLimit=1 -XX:CompileCommandFile=${DIR}/compile-commands.txt"

export JVM_OPTS=${RUN_JVM_OPTS}
for ((i=0;i<1;++i));
do
    echo "running for ${i} round"
    if [ "${app_type}"x = "sssp-default"x ]
    then
        echo "source oid "${source_oid}
        GLOG_v=2 mpirun -n ${num_worker} \
                    -envlist GLOG_v,JVM_OPTS,LD_PRELOAD,input_type \
                    ${DIR}/build/run_java_app ${serialize_opt} ${prefix} ${app_class} ${app_context_class} ${source_oid}
    elif [[ "${app_type}"x = "sssp-parallel"x ]] 
    then
        echo "source oid "${source_oid}
        GLOG_v=1 mpirun -n ${num_worker} \
                    -envlist GLOG_v,JVM_OPTS,,LD_PRELOAD,input_type \
                    ${DIR}/build/run_java_app ${serialize_opt} ${prefix} ${app_class} ${app_context_class} ${source_oid} ${thread_num}
    elif [ "${app_type}"x = "traverse"x ]
    then
        GLOG_v=1 mpirun -n ${num_worker} \
                    -envlist GLOG_v,JVM_OPTS,LD_PRELOAD,input_type \
                    ${DIR}/build/run_java_app ${serialize_opt} ${prefix} ${app_class} ${app_context_class} ${maxiter}
    elif [ "${app_type}"x = ""x ]
    then
        echo "unrecognized app type"
    fi
    
done
