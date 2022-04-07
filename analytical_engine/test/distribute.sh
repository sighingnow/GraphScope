hostfile=~/hostfile
for host in `cat $hostfile`;
do
   echo $host
   scp ~/gs/analytical_engine/java/graphx-on-graphscope/target/graphx-on-graphscope-0.1-shaded.jar graphscope@${host}:~/gs/analytical_engine/java/graphx-on-graphscope/target/graphx-on-graphscope-0.1-shaded.jar
   scp ~/gs/analytical_engine/java/grape-runtime/target/grape-runtime-0.1-shaded.jar graphscope@${host}:/opt/graphscope/lib/grape-runtime-0.1-shaded.jar
   scp ~/gs/analytical_engine/java/grape-runtime/target/native/libgrape-jni.so graphscope@${host}:/opt/graphscope/lib/libgrape-jni.so
   scp ~/gs/analytical_engine/build/graphx_runner graphscope@${host}:/home/graphscope/gs/analytical_engine/build/graphx_runner
done
