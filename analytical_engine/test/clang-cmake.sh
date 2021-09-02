#! /bin/bash

echo $PWD

export LLVM11_HOME=${LLVM11_HOME:-/usr/lib/llvm-11}
export CC=${LLVM11_HOME}/bin/clang
export CXX=${LLVM11_HOME}/bin/clang++
export LD_PRELOAD=${LD_PRELOAD}:${JAVA_HOME}/jre/lib/amd64/libjsig.so

CXX_FLAGS="-flto -fforce-emit-vtables -lpthread"
LINKER_FLAGS="-fuse-ld=${LLVM11_HOME}/bin/ld.lld -Xlinker -mllvm=-lto-embed-bitcode"

cmake  -DCMAKE_JNI_LINKER_FLAGS="$LINKER_FLAGS" -DCMAKE_CXX_FLAGS="$CXX_FLAGS" $@
