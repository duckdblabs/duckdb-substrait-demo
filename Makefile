.PHONY: duckdb clean main

all: duckdb main

clean:
	rm -rf build
	cd duckdb && make clean

duckdb:
	cd duckdb && BUILD_TPCH=1 DISABLE_SANITIZER=1 make

main:
	mkdir -p build
	cd build && cmake -DCMAKE_BUILD_TYPE=Debug .. && cmake --build .
	build/duckdb_substrait

format:
	clang-format --sort-includes=0 -style=file -i main.cpp src/*.cpp src/include/*.hpp
	cmake-format -i CMakeLists.txt