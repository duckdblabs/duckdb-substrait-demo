include_directories(${CMAKE_SOURCE_DIR}/third_party/catch)
include_directories(${CMAKE_SOURCE_DIR}/tests)

# Compile tests
set(TEST_SOURCES ${CMAKE_SOURCE_DIR}/tests/test.cpp)
add_executable(unit_test ${TEST_SOURCES})
target_link_libraries(unit_test duckdb_substrait)