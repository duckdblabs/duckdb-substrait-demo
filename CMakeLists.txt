cmake_minimum_required(VERSION 2.8.12)
project(duckdb_substrait)

set(CMAKE_CXX_STANDARD 14)

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
# add extra include directories
include_directories(cmake-build-debug/)
file(GLOB ProtoFiles "substrait/binary/*.proto")
protobuf_generate_cpp(ProtoSources ProtoHeaders ${ProtoFiles})
add_library(substrait STATIC ${ProtoSources} ${ProtoHeaders})

target_link_libraries(duckdb_substrait substrait)
target_link_libraries(duckdb_substrait test_helpers)
target_link_libraries(duckdb_substrait ${Protobuf_LIBRARIES})

add_subdirectory("tests")
