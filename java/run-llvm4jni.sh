#! /bin/bash

DIR=$(pushd $(dirname ${BASH_SOURCE[0]}) > /dev/null && pwd && popd > /dev/null)


if [[ -d "$JAVA_HOME" ]]; then
    JAVA=$JAVA_HOME/bin/java
else
    JAVA=$(which java)
fi

if [[ ! -x "$JAVA" ]]; then
    echo "\$JAVA_HOME/bin/java points to $JAVA, which is not an executable"
    exit 1
fi

VERSION=0.1
JAR=${DIR}/target/llvm4jni-${VERSION}-jar-with-dependencies.jar

if [[ ! -f $JAR ]];
then
    echo "Need to run 'mvn clean install' in the root directory"
    exit 1
fi

MAIN_CLASS=com.alibaba.llvm4jni.Main
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${DIR}/../llvm/target/native
$JAVA $VM_OPTS -Djava.library.path=${DIR}/../llvm/target/native -cp ${DIR}/target/classes:$JAR:$EXTRA_CP $MAIN_CLASS $@
