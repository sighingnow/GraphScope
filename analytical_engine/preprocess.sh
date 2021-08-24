DIR=$(pushd $(dirname $BASH_SOURCE[0]) > /dev/null && pwd && popd > /dev/null)

M2_REPO_GRAPE=~/.m2/repository/com/alibaba/grape

grape_demo_jar=${M2_REPO_GRAPE}/grape-demo/0.1/grape-demo-0.1-jar-with-dependencies.jar
grape_sdk_jar=${M2_REPO_GRAPE}/grape-sdk/0.1/grape-sdk-0.1-jar-with-dependencies.jar
grape_processor_jar=${M2_REPO_GRAPE}/grape-processor/0.1/grape-processor-0.1-jar-with-dependencies.jar

PRE_CP=${grape_demo_jar}:${grape_sdk_jar}:${grape_processor_jar}
export JVM_OPTS="-Djava.class.path=${PRE_CP}" # -XX:+TraceClassLoading"

task_main_class=com.alibaba.grape.TraverseMainClass
/usr/local/bin/run_java_app_preprocess \
                        ${task_main_class} \
                        ${grape_demo_jar} \
                        /tmp/java_pie.conf /tmp/gs/gs-ffi123 vfile efile 
