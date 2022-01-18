#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"

#include "tpch-extension.hpp"

#include "compare_result.hpp"

#include "substrait/plan.pb.h"

#include "duckdb_to_substrait.hpp"
#include "substrait_to_duckdb.hpp"

#include <string>
#include <fstream>
#include <cassert>
#include "duckdb/main/materialized_query_result.hpp"

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

using namespace std;
using namespace duckdb;

auto tpch_db = make_unique<DuckDB>();
auto tpch_conn = make_unique<Connection>(*tpch_db);
auto tpch_con = *tpch_conn;
bool initialize = true;
bool optimized_plans = true;

bool debug = false;

bool CompareQueryResults(QueryResult &actual_result, QueryResult &roundtrip_result) {
	// actual_result compare the success state of the results
	if (actual_result.success != roundtrip_result.success) {
		return false;
	}
	if (!actual_result.success) {
		return actual_result.error == roundtrip_result.error;
	}
	// FIXME: How to name expression?
	//	// compare names
	//	if (names != other.names) {
	//		return false;
	//	}
	// compare types
	if (actual_result.types != roundtrip_result.types) {
		return false;
	}
	// now compare the actual values
	// fetch chunks
	while (true) {
		auto lchunk = actual_result.Fetch();
		auto rchunk = roundtrip_result.Fetch();
		if (!lchunk && !rchunk) {
			return true;
		}
		if (!lchunk || !rchunk) {
			return false;
		}
		if (lchunk->size() == 0 && rchunk->size() == 0) {
			return true;
		}
		if (lchunk->size() != rchunk->size()) {
			return false;
		}
		D_ASSERT(lchunk->ColumnCount() == rchunk->ColumnCount());
		for (idx_t col = 0; col < rchunk->ColumnCount(); col++) {
			for (idx_t row = 0; row < rchunk->size(); row++) {
				auto lvalue = lchunk->GetValue(col, row);
				auto rvalue = rchunk->GetValue(col, row);
				if (lvalue != rvalue) {
					return false;
				}
			}
		}
	}
}

static void roundtrip_query(duckdb::Connection &con, const string &query) {
	DuckDBToSubstrait transformer_d2s;
	auto actual_result = con.Query(query);
	con.context->config.enable_optimizer = optimized_plans;
	auto query_plan = con.context->ExtractPlan(query);
	if (debug) {
		query_plan->Print();
	}

	transformer_d2s.TransformPlan(*query_plan);

	string serialized;
	transformer_d2s.SerializeToString(serialized);

	substrait::Plan splan2;
	splan2.ParseFromString(serialized);
	SubstraitToDuckDB transformer_s2d(con, splan2);
	auto duckdb_rel = transformer_s2d.TransformPlan(splan2);
	splan2.Clear();
	if (debug) {
		duckdb_rel->Print();
	}
	auto round_trip_result = duckdb_rel->Execute();
	REQUIRE(CompareQueryResults(*actual_result, *round_trip_result));
}

TEST_CASE("SELECT *", "[Simple]") {
	auto db = make_unique<DuckDB>();
	auto conn = make_unique<Connection>(*db);
	auto con = *conn;
	con.Query("CREATE TABLE person (name text,money int);");
	con.Query("insert into person values ('Pedro', 10);");

	roundtrip_query(con, "select * from person");
}

TEST_CASE("Projection", "[Simple]") {
	auto db = make_unique<DuckDB>();
	auto conn = make_unique<Connection>(*db);
	auto con = *conn;
	con.Query("CREATE TABLE person (name text,money int);");
	con.Query("insert into person values ('Pedro', 10);");

	roundtrip_query(con, "select name from person");
}

TEST_CASE("Filter", "[Simple]") {
	auto db = make_unique<DuckDB>();
	auto conn = make_unique<Connection>(*db);
	auto con = *conn;
	con.Query("CREATE TABLE person (name text,money int);");
	con.Query("insert into person values ('Pedro', 10);");
	con.Query("insert into person values ('Richard', 20);");

	roundtrip_query(con, "select * from person where name = 'Pedro'");
}

TEST_CASE("Aggregation", "[Simple]") {
	auto db = make_unique<DuckDB>();
	auto conn = make_unique<Connection>(*db);
	auto con = *conn;
	con.Query("CREATE TABLE person (name text,money int);");
	con.Query("insert into person values ('Pedro', 10);");
	con.Query("insert into person values ('Richard', 20);");
	con.Query("insert into person values ('Pedro', 20);");

	roundtrip_query(con, "select SUM(money) from person");
}

TEST_CASE("Aggregation on Decimal", "[Simple]") {
	auto db = make_unique<DuckDB>();
	auto conn = make_unique<Connection>(*db);
	auto con = *conn;
	con.Query("CREATE TABLE person (money DECIMAL(5,2));");
	con.Query("insert into person values (105.35);");
	con.Query("insert into person values (1.11);");
	roundtrip_query(con, "select SUM(money * 2) as double_money from person");
}

TEST_CASE("Aggregation and Filter", "[Simple]") {
	auto db = make_unique<DuckDB>();
	auto conn = make_unique<Connection>(*db);
	auto con = *conn;
	con.Query("CREATE TABLE person (name text,money decimal);");
	con.Query("insert into person values ('Pedro', 10);");
	con.Query("insert into person values ('Richard', 20);");
	con.Query("insert into person values ('Pedro', 20);");

	roundtrip_query(con, "select SUM(money) from person where name = 'Pedro'");
}

TEST_CASE("String Filter", "[Simple]") {
	auto db = make_unique<DuckDB>();
	auto conn = make_unique<Connection>(*db);
	auto con = *conn;
	con.Query("CREATE TABLE person (name text);");
	con.Query("insert into person values ('bla special bla requests bla');");
	con.Query("insert into person values ('special requests')");
	con.Query("insert into person values ('bla')");

	roundtrip_query(con, "select * from person where name not like '%special%requests%'");
}

void test_tpch(int query_number) {
	if (initialize) {
		initialize = false;
		tpch_con.Query("call dbgen(sf=0.1)");
	}
	auto query = TPCHExtension::GetQuery(query_number);
	roundtrip_query(tpch_con, query);
}

TEST_CASE("TPC-H Q 01", "[tpch]") {
	test_tpch(1);
}

// TEST_CASE("TPC-H Q 02", "[tpch]") {
//	test_tpch(2);
// }

TEST_CASE("TPC-H Q 03", "[tpch]") {
	test_tpch(3);
}

// TEST_CASE("TPC-H Q 04", "[tpch]") {
//	test_tpch(4);
// }

TEST_CASE("TPC-H Q 05", "[tpch]") {
	test_tpch(5);
}

TEST_CASE("TPC-H Q 06", "[tpch]") {
	test_tpch(6);
}

TEST_CASE("TPC-H Q 07", "[tpch]") {
	test_tpch(7);
}

TEST_CASE("TPC-H Q 08", "[tpch]") {
	test_tpch(8);
}

TEST_CASE("TPC-H Q 09", "[tpch]") {
	test_tpch(9);
}

TEST_CASE("TPC-H Q 10", "[tpch]") {
	test_tpch(10);
}

TEST_CASE("TPC-H Q 11", "[tpch]") {
	test_tpch(11);
}

TEST_CASE("TPC-H Q 12", "[tpch]") {
	test_tpch(12);
}

TEST_CASE("TPC-H Q 13", "[tpch]") {
	test_tpch(13);
}

TEST_CASE("TPC-H Q 14", "[tpch]") {
	test_tpch(14);
}

// TEST_CASE("TPC-H Q 15", "[tpch]") {
//	test_tpch(15);
// }
//
// TEST_CASE("TPC-H Q 16", "[tpch]") {
//	test_tpch(16);
// }
//
// TEST_CASE("TPC-H Q 17", "[tpch]") {
//	test_tpch(17);
// }
//
// TEST_CASE("TPC-H Q 18", "[tpch]") {
//	test_tpch(18);
// }
//
// TEST_CASE("TPC-H Q 19", "[tpch]") {
//	test_tpch(19);
// }
//
// TEST_CASE("TPC-H Q 20", "[tpch]") {
//	test_tpch(20);
// }
//
// TEST_CASE("TPC-H Q 21", "[tpch]") {
//	test_tpch(21);
// }
//
// TEST_CASE("TPC-H Q 22", "[tpch]") {
//	test_tpch(22);
// }

int main(int argc, char *argv[]) {
	// global setup...

	int result = Catch::Session().run(argc, argv);

	// global clean-up...

	return result;
}
