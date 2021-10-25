.PHONY: duckdb clean main

all: duckdb main

clean:
	rm -rf build
	cd ../.. && make clean

duckdb:
	cd duckdb && BUILD_TPCH=1 DISABLE_SANITIZER=1 make debug

main:
	mkdir -p build
	cd build && cmake -DCMAKE_BUILD_TYPE=Debug .. && cmake --build .
	build/duckdb_arrowir


