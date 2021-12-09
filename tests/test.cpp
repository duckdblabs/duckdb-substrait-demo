#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"

#include "tpch-extension.hpp"

#include "compare_result.hpp"

#include "plan.pb.h"

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
namespace substrait = io::substrait;

bool CompareQueryResults(QueryResult &first, QueryResult &second) {
	// first compare the success state of the results
	if (first.success != second.success) {
		return false;
	}
	if (!first.success) {
		return first.error == second.error;
	}
	// FIXME: How to name expression?
	//	// compare names
	//	if (names != other.names) {
	//		return false;
	//	}
	// compare types
//	if (first.types != second.types) {
//		return false;
//	}
	// now compare the actual values
	// fetch chunks
	while (true) {
		auto lchunk = first.Fetch();
		auto rchunk = second.Fetch();
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

static void roundtrip_query(duckdb::Connection &con, const string& query) {
	DuckDBToSubstrait transformer_d2s;
	auto actual_result = con.Query(query);

	auto query_plan = con.context->ExtractPlan(query);
	transformer_d2s.TransformPlan(*query_plan);

	string serialized;
	transformer_d2s.SerializeToString(serialized);

	substrait::Plan splan2;
	splan2.ParseFromString(serialized);
	SubstraitToDuckDB transformer_s2d(con, splan2);
	auto duckdb_rel = transformer_s2d.TransformPlan(splan2);
	splan2.Clear();
	auto round_trip_result = duckdb_rel->Execute();
	REQUIRE(CompareQueryResults(*actual_result, *round_trip_result));
}

TEST_CASE("SELECT *", "[Simple]" )
{
	auto db = make_unique<DuckDB>();
	auto conn = make_unique<Connection>(*db);
	auto con = *conn;
	con.Query("CREATE TABLE person (name text,money int);");
	con.Query("insert into person values ('Pedro', 10);");

	roundtrip_query(con, "select * from person");
}

TEST_CASE("Projection", "[Simple]" )
{
	auto db = make_unique<DuckDB>();
	auto conn = make_unique<Connection>(*db);
	auto con = *conn;
	con.Query("CREATE TABLE person (name text,money int);");
	con.Query("insert into person values ('Pedro', 10);");

	roundtrip_query(con, "select name from person");
}

TEST_CASE("Filter", "[Simple]" )
{
	auto db = make_unique<DuckDB>();
	auto conn = make_unique<Connection>(*db);
	auto con = *conn;
	con.Query("CREATE TABLE person (name text,money int);");
	con.Query("insert into person values ('Pedro', 10);");
	con.Query("insert into person values ('Richard', 20);");

	roundtrip_query(con, "select * from person where name = 'Pedro'");
}

TEST_CASE("Aggregation", "[Simple]" )
{
	auto db = make_unique<DuckDB>();
	auto conn = make_unique<Connection>(*db);
	auto con = *conn;
	con.Query("CREATE TABLE person (name text,money int);");
	con.Query("insert into person values ('Pedro', 10);");
	con.Query("insert into person values ('Richard', 20);");
	con.Query("insert into person values ('Pedro', 20);");

	roundtrip_query(con, "select SUM(money) from person");
}

TEST_CASE("Aggregation and Filter", "[Simple]" )
{
	auto db = make_unique<DuckDB>();
	auto conn = make_unique<Connection>(*db);
	auto con = *conn;
	con.Query("CREATE TABLE person (name text,money decimal);");
	con.Query("insert into person values ('Pedro', 10);");
	con.Query("insert into person values ('Richard', 20);");
	con.Query("insert into person values ('Pedro', 20);");

	roundtrip_query(con, "select SUM(money) from person where name = 'Pedro'");
}

TEST_CASE("TPC-H", "[tpch]" )
{
//	vector<uint8_t> queries {1,3,5,6,7,8,9,10,11,12,13,14};
	vector<uint8_t> queries {6};

	auto db = make_unique<DuckDB>();
	auto conn = make_unique<Connection>(*db);
	auto con = *conn;
	con.Query("call dbgen(sf=0.1)");

	for (auto& query_number: queries){
		auto query = TPCHExtension::GetQuery(query_number);
		roundtrip_query(con, query);
	}

}

//SELECT
//    sum(l_extendedprice * l_discount) AS revenue
//FROM
//    lineitem
//WHERE
//    l_shipdate >= CAST('1994-01-01' AS date)
//    AND l_shipdate < CAST('1995-01-01' AS date)
//    AND l_discount BETWEEN 0.05
//    AND 0.07
//    AND l_quantity < 24;


int main( int argc, char* argv[] ) {
  // global setup...

  int result = Catch::Session().run( argc, argv );

  // global clean-up...

  return result;
}
