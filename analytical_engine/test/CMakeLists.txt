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