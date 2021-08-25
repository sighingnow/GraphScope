DIR=$(pushd $(dirname $BASH_SOURCE[0]) > /dev/null && pwd && popd > /dev/null)

M2_REPO_GRAPE=~/.m2/repository/com/alibaba/grape

#grape_demo_jar=${M2_REPO_GRAPE}/grape-demo/0.1/grape-demo-0.1-jar-with-dependencies.jar
graphscope_demo_jar=${M2_REPO_GRAPE}/graphscope-demo/0.1/graphscope-demo-0.1-jar-with-dependencies.jar
grape_sdk_jar=${M2_REPO_GRAPE}/grape-sdk/0.1/grape-sdk-0.1-jar-with-dependencies.jar
grape_processor_jar=${M2_REPO_GRAPE}/grape-processor/0.1/grape-processor-0.1-jar-with-dependencies.jar

PRE_CP=${graphscope_demo_jar}:${grape_sdk_jar}:${grape_processor_jar}
export JVM_OPTS="-Djava.class.path=${PRE_CP}" # -XX:+TraceClassLoading"

task_main_class=io.graphscope.example.TraverseMain
GLOG_v=10 /usr/local/bin/run_java_app_preprocess \
                        ${task_main_class} \
                        ${graphscope_demo_jar} \
                        /tmp/gs/session_qfdwswvb/0ee942126811bb9d7236a4f0d8148a833748bf6a7ffd7d4c3bff99a5c9d69e4f/java_pie.conf \
                        /tmp/lei-test/ffi
                        #/tmp/gs/session_qfdwswvb/0ee942126811bb9d7236a4f0d8148a833748bf6a7ffd7d4c3bff99a5c9d69e4f/gs-ffi1934764597241356523
