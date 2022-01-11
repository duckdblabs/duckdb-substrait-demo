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

file(GLOB ProtoFiles substrait/proto/substrait/extensions/*.proto
     substrait/proto/substrait/*.proto)

# directory of the generated files after compilation
set(PROTO_META_BASE_DIR ${CMAKE_CURRENT_BINARY_DIR})

# specifies the total search path when compiling.
list(APPEND PROTO_FLAGS -I${CMAKE_CURRENT_SOURCE_DIR})

file(TO_NATIVE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/substrait/proto/
     PROTOMODEL_PATH)

foreach(FIL ${ProtoFiles})

  get_filename_component(FIL_WE ${FIL} NAME_WE)

  string(REGEX REPLACE ".+/(.+)\\..*" "\\1" FILE_NAME ${FIL})
  string(REGEX REPLACE "(.+)\\${FILE_NAME}.*" "\\1" FILE_PATH ${FIL})

  string(
    REGEX
      MATCH
      "(/substrait/proto/substrait/extensions.*|/substrait/proto/substrait.*)"
      OUT_PATH ${FILE_PATH})

  set(PROTO_SRCS "${CMAKE_CURRENT_BINARY_DIR}${OUT_PATH}${FIL_WE}.pb.cc")
  set(PROTO_HDRS "${CMAKE_CURRENT_BINARY_DIR}${OUT_PATH}${FIL_WE}.pb.h")

  execute_process(
    COMMAND
      ${PROTOBUF_PROTOC_EXECUTABLE} ${PROTO_FLAGS}
      --proto_path=${PROTOMODEL_PATH} --cpp_out=${PROTO_META_BASE_DIR} ${FIL})
  # message("Copying " ${PROTO_SRCS} " to " ${FILE_PATH})

  # file(COPY ${PROTO_SRCS} DESTINATION ${FILE_PATH}) file(COPY ${PROTO_HDRS}
  # DESTINATION ${FILE_PATH})
  list(APPEND PROTO_SRCS_LIST ${PROTO_SRCS})
  list(APPEND PROTO_HDRS_LIST ${PROTO_HDRS})

endforeach()

include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR}/substrait/proto)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/substrait/proto/extensions)
# add_library(substrait STATIC ${PROTO_SRCS_LIST} ${CMAKE_CURRENT_BINARY_DIR}
# ${CMAKE_CURRENT_BINARY_DIR}/substrait/proto/substrait/extensions/extensions.pb.h)
add_library(substrait STATIC ${PROTO_SRCS_LIST} ${CMAKE_CURRENT_BINARY_DIR}
                             ${PROTO_HDRS_LIST})

include_directories(${CMAKE_CURRENT_BINARY_DIR}/substrait/proto/substrait)
target_link_libraries(duckdb_substrait substrait)
target_link_libraries(duckdb_substrait test_helpers)
target_link_libraries(duckdb_substrait ${Protobuf_LIBRARIES})

add_subdirectory("tests")
