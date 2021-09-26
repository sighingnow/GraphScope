cmake_minimum_required(VERSION 2.8)
project(VINEYARD-JNI)

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


add_library(vineyard-jni SHARED ${SOURCES})
target_compile_definitions(vineyard-jni PUBLIC PUBLIC ENABLE_JAVA_SDK)
target_link_libraries(vineyard-jni ${CMAKE_JNI_LINKER_FLAGS} ${VINEYARD_LIBRARIES} ${GRAPHSCOPE_ANALYTICAL_INCLUDE_DIRS})
set_target_properties(vineyard-jni PROPERTIES LINKER_LANGUAGE CXX)
target_compile_features(vineyard-jni PRIVATE cxx_std_14)


add_custom_command(TARGET vineyard-jni POST_BUILD COMMAND ${CMAKE_COMMAND} -E make_directory "${CMAKE_CURRENT_SOURCE_DIR}/target/classes/natives/linux_64/")
add_custom_command(TARGET vineyard-jni
                   POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:vineyard-jni> "${CMAKE_CURRENT_SOURCE_DIR}/target/classes/natives/linux_64/")

install(TARGETS vineyard-jni
        EXPORT VineyardJNITargets
        LIBRARY DESTINATION lib)