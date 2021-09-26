cmake_minimum_required(VERSION 2.8)
project(GRAPHSCOPE-JNI)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -g -std=gnu++14 -Wall")

file(GLOB SOURCES "target/generated-sources/annotations/*.cc" "src/main/cpp/*.cc")

set(JAVA_AWT_LIBRARY NotNeeded)
set(JAVA_AWT_INCLUDE_PATH NotNeeded)
find_package(JNI REQUIRED)
include_directories(SYSTEM ${JAVA_INCLUDE_PATH})
include_directories(SYSTEM ${JAVA_INCLUDE_PATH2})
include_directories("src/main/cpp")


find_package(vineyard 0.2.6 REQUIRED)
include_directories(${VINEYARD_INCLUDE_DIRS})

find_package(graphscope-analytical)
include_directories(SYSTEM ${GRAPHSCOPE_ANALYTICAL_INCLUDE_DIRS})


add_library(graphscope-jni SHARED ${SOURCES})
target_compile_definitions(graphscope-jni PUBLIC PUBLIC ENABLE_JAVA_SDK)
target_link_libraries(graphscope-jni ${CMAKE_JNI_LINKER_FLAGS} ${VINEYARD_LIBRARIES} ${GRAPHSCOPE_ANALYTICAL_INCLUDE_DIRS})
set_target_properties(graphscope-jni PROPERTIES LINKER_LANGUAGE CXX)
target_compile_features(graphscope-jni PRIVATE cxx_std_14)


add_custom_command(TARGET graphscope-jni POST_BUILD COMMAND ${CMAKE_COMMAND} -E make_directory "${CMAKE_CURRENT_SOURCE_DIR}/target/classes/natives/linux_64/")
add_custom_command(TARGET graphscope-jni
                   POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:graphscope-jni> "${CMAKE_CURRENT_SOURCE_DIR}/target/classes/natives/linux_64/")

install(TARGETS graphscope-jni
        EXPORT VineyardJNITargets
        LIBRARY DESTINATION lib)