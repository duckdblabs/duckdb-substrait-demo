cmake_minimum_required(VERSION 2.8.12)
project(duckdb_substrait)

set(CMAKE_CXX_STANDARD 17)

if("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
  add_compile_options(-Wall -Werror -pedantic -fsanitize=address)
endif()

include_directories(src/include)

include_directories(duckdb/src/include)
include_directories(duckdb/extension/tpch/include)
include_directories(duckdb/test/include)

include_directories(build)

link_directories(duckdb/build/release/src)
link_directories(duckdb/build/release/extension/tpch)
link_directories(duckdb/build/release/test/helpers)

add_library(duckdb_substrait src/duckdb_to_substrait.cpp
                             src/substrait_to_duckdb.cpp)

target_link_libraries(duckdb_substrait duckdb)
target_link_libraries(duckdb_substrait tpch_extension)

if("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
  target_link_libraries(duckdb_substrait -fsanitize=address)
endif()

find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})

include_directories(substrait/gen/proto/cpp)

set(SUBSTRAIT_PATH substrait/gen/proto/cpp/substrait)
set(SUBSTRAIT_SOURCES
    ${SUBSTRAIT_PATH}/capabilities.pb.cc
    ${SUBSTRAIT_PATH}/expression.pb.cc
    ${SUBSTRAIT_PATH}/function.pb.cc
    ${SUBSTRAIT_PATH}/parameterized_types.pb.cc
    ${SUBSTRAIT_PATH}/plan.pb.cc
    ${SUBSTRAIT_PATH}/relations.pb.cc
    ${SUBSTRAIT_PATH}/type.pb.cc
    ${SUBSTRAIT_PATH}/type_expressions.pb.cc
    ${SUBSTRAIT_PATH}/extensions/extensions.pb.cc)
set(SUBSTRAIT_HEADERS
    ${SUBSTRAIT_PATH}/capabilities.pb.h
    ${SUBSTRAIT_PATH}/expression.pb.h
    ${SUBSTRAIT_PATH}/function.pb.h
    ${SUBSTRAIT_PATH}/parameterized_types.pb.h
    ${SUBSTRAIT_PATH}/plan.pb.h
    ${SUBSTRAIT_PATH}/relations.pb.h
    ${SUBSTRAIT_PATH}/type.pb.h
    ${SUBSTRAIT_PATH}/type_expressions.pb.h
    ${SUBSTRAIT_PATH}/extensions/extensions.pb.h)

add_library(substrait STATIC ${SUBSTRAIT_SOURCES} ${SUBSTRAIT_HEADERS})

include_directories(${CMAKE_CURRENT_BINARY_DIR}/substrait/proto/substrait)
target_link_libraries(duckdb_substrait substrait)
target_link_libraries(duckdb_substrait test_helpers)
target_link_libraries(duckdb_substrait ${Protobuf_LIBRARIES})

add_subdirectory("tests")
