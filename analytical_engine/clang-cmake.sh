#! /bin/bash

echo $PWD

export LLVM11_HOME=${LLVM11_HOME:-/usr/lib/llvm-11}
export CC=${LLVM11_HOME}/bin/clang
export CXX=${LLVM11_HOME}/bin/clang++

CXX_FLAGS="-flto -fforce-emit-vtables"
LINKER_FLAGS="-fuse-ld=${LLVM11_HOME}/bin/ld.lld -Xlinker -mllvm=-lto-embed-bitcode"
#-Xlinker -lpthread
# -mllvm=-lto-embed-bitcode 
INSTALL_PREFIX=/usr/local
NETWORKX=OFF
BUILD_TEST=OFF
#-DCMAKE_JNI_LINKER_FLAGS="$LINKER_FLAGS"
#  -DCMAKE_CXX_FLAGS="$CXX_FLAGS"
cmake  -DCMAKE_JNI_LINKER_FLAGS="$LINKER_FLAGS" -DCMAKE_CXX_FLAGS="$CXX_FLAGS" -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} -DNETWORKX=${NETWORKX} -DBUILD_TESTS=${BUILD_TEST}  $@

