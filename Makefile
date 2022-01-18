.PHONY: clean main

all: initialize-sub substrait-gen main

clean:
	rm -rf build
	rm -rf duckdb
	rm -rf substrait

initialize-sub:
	git submodule init
	git submodule update --remote --merge

substrait-gen:
	cd substrait && buf generate

main:
	mkdir -p build
	cd build && cmake -G "Ninja" -DCMAKE_BUILD_TYPE=Debug .. && cmake --build .

format:
	clang-format --sort-includes=0 -style=file -i src/*.cpp src/include/*.hpp tests/*.cpp
	cmake-format -i CMakeLists.txt