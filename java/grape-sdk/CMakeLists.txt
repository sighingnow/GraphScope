cmake_minimum_required(VERSION 2.8)
project(GRAPE-LITE-JNI)

# ------------------------------------------------------------------------------
# cmake configs
# ------------------------------------------------------------------------------

#include(CheckLibraryExists)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall -O3 -g -fopenmp -Werror")
#if (APPLE)
#    SET(CMAKE_MACOSX_RPATH ON)
#else ()
#    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp -Werror")
#endif ()
#SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g -fprofile-arcs -ftest-coverage")
#SET(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -g")

#if (WITH_ASAN)
#    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer -O1")
#    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
#endif()

file(GLOB SOURCES "target/generated-sources/annotations/*.cc" "src/test/cpp/*.cc")
include_directories("src/main/cpp")
# find mpi---------------------------------------------------------------------
#find_package(MPI REQUIRED)
#include_directories(SYSTEM ${MPI_CXX_INCLUDE_PATH})
# find jni---------------------------------------------------------------------
find_package(JNI REQUIRED)
include_directories(SYSTEM ${JAVA_INCLUDE_PATH})
include_directories(SYSTEM ${JAVA_INCLUDE_PATH2})


# find vineyard ---------------------------------------------------------------
#find_package(vineyard 0.2.6 REQUIRED)
#include_directories(${VINEYARD_INCLUDE_DIRS})

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
# find libgrape-lite-------------------------------------------------------------------
find_package(libgrapelite REQUIRED)
include_directories(SYSTEM ${LIBGRAPELITE_INCLUDE_DIRS})

# find graphscope analytical engine---------------------------------------------
find_package(graphscope-analytical)
include_directories(SYSTEM ${GRAPHSCOPE_ANALYTICAL_INCLUDE_DIRS})
# find libgrape-lite
#find_package(libgrapelite REQUIRED)
#include_directories(${LIBGRAPELITE_INCLUDE_DIRS})
find_package(vineyard 0.2.6 REQUIRED)
include_directories(${VINEYARD_INCLUDE_DIRS})

#TDOD: Put graphscope related dependencies into relative path, when merged into graphscope
add_library(grape-lite-jni SHARED ${SOURCES})
target_compile_definitions(grape-lite-jni PUBLIC PUBLIC ENABLE_JAVA_SDK)
target_include_directories(grape-lite-jni PRIVATE target/generated-sources/annotations/ src/main/cpp/)
target_link_libraries(grape-lite-jni ${JNI_LIBRARIES} ${LIBGRAPELITE_LIBRARIES} ${VINEYARD_LIBRARIES}
                                      ${GFLAGS_LIBRARIES} ${CMAKE_DL_LIBS} ${CMAKE_JNI_LINKER_FLAGS} ${GLOG_LIBRARIES})
set_target_properties(grape-lite-jni PROPERTIES LINKER_LANGUAGE CXX)
target_compile_features(grape-lite-jni PRIVATE cxx_std_14)

add_custom_command(TARGET grape-lite-jni POST_BUILD COMMAND ${CMAKE_COMMAND} -E make_directory "${CMAKE_CURRENT_SOURCE_DIR}/target/classes/natives/linux_64/")
add_custom_command(TARGET grape-lite-jni
                   POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:grape-lite-jni> "${CMAKE_CURRENT_SOURCE_DIR}/target/classes/natives/linux_64/")

install(TARGETS grape-lite-jni
        EXPORT GrapeJNITargets
        LIBRARY DESTINATION lib)