cmake_minimum_required(VERSION 2.8)
project(JAVA-APP-JNI)

# ------------------------------------------------------------------------------
# cmake configs
# ------------------------------------------------------------------------------

#include(CheckLibraryExists)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++14 -Wall -O3 -g")

file(GLOB_RECURSE JNI_USER_FILES "$ENV{GRAPE_GEN_PATH}/SOURCE_OUTPUT/*.cc")

# find jni---------------------------------------------------------------------
find_package(JNI REQUIRED)
include_directories(SYSTEM ${JAVA_INCLUDE_PATH})
include_directories(SYSTEM ${JAVA_INCLUDE_PATH2})

# find libgrape-lite---------------------------------------------------------------------
find_package(libgrapelite REQUIRED)
include_directories(${LIBGRAPELITE_INCLUDE_DIRS})
# find mpi--------------------------------------------------------------------
find_package(MPI REQUIRED)
include_directories(SYSTEM ${MPI_CXX_INCLUDE_PATH})

# find glog---------------------------------------------------------------------
include("cmake/FindGlog.cmake")
if (NOT GLOG_FOUND)
    message(FATAL_ERROR "glog not found, please install the glog library")
else ()
    include_directories(SYSTEM ${GLOG_INCLUDE_DIRS})
endif ()

# find gflags-------------------------------------------------------------------
include("cmake/FindGFlags.cmake")
if (NOT GFLAGS_FOUND)
    message(STATUS "gflags not found, build without gflags")
else ()
    include_directories(SYSTEM ${GFLAGS_INCLUDE_DIRS})
endif ()

add_library(java-app-jni ${JNI_USER_FILES})
target_compile_definitions(java-app-jni PUBLIC -DGRAPE_SDK_CPP_GRAPE_GEN_DEF)
set_target_properties(java-app-jni PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(java-app-jni PRIVATE $ENV{GRAPE_GEN_PATH}/SOURCE_OUTPUT/)
target_link_libraries(java-app-jni ${MPI_CXX_LIBRARIES} ${JNI_LIBRARIES}
                                ${GFLAGS_LIBRARIES} ${GLOG_LIBRARIES} ${CMAKE_DL_LIBS} ${CMAKE_JNI_LINKER_FLAGS})
target_compile_features(java-app-jni PRIVATE cxx_std_14)

add_executable(run_java_app test/run_java_app.cc)
target_include_directories(run_java_app PRIVATE ${LIBGRAPELITE_INCLUDE_DIRS}/grape/analytical_apps utils apps)
target_link_libraries(run_java_app ${LIBGRAPELITE_LIBRARIES} ${GFLAGS_LIBRARIES} ${MPI_CXX_LIBRARIES} ${JNI_LIBRARIES}
                                    ${CMAKE_DL_LIBS} ${GLOG_LIBRARIES} ${JAVA_JVM_LIBRARY} ${CMAKE_JNI_LINKER_FLAGS})
add_executable(run_java_app_preprocess test/run_java_app_preprocess.cc)
target_include_directories(run_java_app PRIVATE ${LIBGRAPELITE_INCLUDE_DIRS}/grape/analytical_apps utils apps)
target_link_libraries(run_java_app ${LIBGRAPELITE_LIBRARIES} ${GFLAGS_LIBRARIES} ${MPI_CXX_LIBRARIES} ${JNI_LIBRARIES}
                                    ${CMAKE_DL_LIBS} ${GLOG_LIBRARIES} ${JAVA_JVM_LIBRARY} ${CMAKE_JNI_LINKER_FLAGS})