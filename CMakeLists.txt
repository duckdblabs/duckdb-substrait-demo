cmake_minimum_required(VERSION 2.8.12)
project(duckdb_substrait)

set(CMAKE_CXX_STANDARD 11)

include_directories(duckdb/src/include)
include_directories(duckdb/extension/tpch/include)
include_directories(duckdb/test/include)

include_directories(build)


link_directories(duckdb/build/debug/src)
link_directories(duckdb/build/debug/extension/tpch)

add_executable(duckdb_substrait main.cpp)
target_link_libraries(duckdb_substrait duckdb)
target_link_libraries(duckdb_substrait tpch_extension)

find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})

file(GLOB ProtoFiles "substrait/binary/*.proto")
PROTOBUF_GENERATE_CPP(ProtoSources ProtoHeaders ${ProtoFiles})
add_library(substrait STATIC ${ProtoSources} ${ProtoHeaders})

target_link_libraries(duckdb_substrait substrait)
target_link_libraries(duckdb_substrait ${Protobuf_LIBRARIES})
