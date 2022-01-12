.PHONY: duckdb clean main

all: substrait-gen duckdb main

clean:
	rm -rf build
	cd duckdb && make clean

substrait-gen:
	cd substrait && buf generate

main:
	mkdir -p build
	cd build && cmake -DCMAKE_BUILD_TYPE=Debug .. && cmake --build .

format:
	clang-format --sort-includes=0 -style=file -i src/*.cpp src/include/*.hpp tests/*.cpp
	cmake-format -i CMakeLists.txt