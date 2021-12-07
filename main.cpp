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

using namespace std;
namespace substrait = io::substrait;
//
//static unique_ptr<duckdb::QueryResult> roundtrip_plan(duckdb::Connection &con, string &q, DuckDBToSubstrait &transformer_d2s) {
//
//	auto dplan = con.context->ExtractPlan(q);
//
//	transformer_d2s.TransformPlan(*dplan);
//
//	string serialized;
//	transformer_d2s.SerializeToString(serialized);
////	transformer_d2s.Clear();
//
//	// readback woo
//	substrait::Plan splan2;
//	splan2.ParseFromString(serialized);
//	//
//	//    printf("\n%s\n", dplan->ToString().c_str());
//	//
//	//    splan2.PrintDebugString();
//
//	SubstraitToDuckDB transformer_s2d(con, splan2);
//	auto duckdb_rel = transformer_s2d.TransformPlan(splan2);
//	splan2.Clear();
//
//	//		con.Query(q)->Print();
//	//
//	//			duckdb_rel->Print();
//	//		    duckdb_rel->Explain()->Print();
//	//
//	//	    duckdb_rel->Execute()->Print();
//
//	return duckdb_rel->Execute();
//}
//
//static void roundtrip_tpch_plan(duckdb::Connection &con, int q_nr) {
//	DuckDBToSubstrait transformer_d2s;
//	auto q = duckdb::TPCHExtension::GetQuery(q_nr);
//
//	auto res = roundtrip_plan(con, q,transformer_d2s);
//
//	// check the results
//	string file_content;
//	getline(ifstream(duckdb::StringUtil::Format("duckdb/extension/tpch/dbgen/answers/sf0.1/q%02d.csv", q_nr)),
//	        file_content, '\0');
//	string compare_result = duckdb::compare_csv(*res, file_content, true);
////	if (!compare_result.empty()) {
////		printf("%s\n", compare_result.c_str());
////		throw runtime_error("TPC-H Query result compare failure. Q:" + to_string(q_nr));
////	} else{
////		auto result = "TPC-H Query Succeded. Q:" +  to_string(q_nr) + "\n";
////		printf("%s", result.c_str());
////	}
//}

static void roundtrip_query(duckdb::Connection &con, string query){
	DuckDBToSubstrait transformer_d2s;
	auto actual_result = con.Query(query);
	auto query_plan = con.context->ExtractPlan(query);
	transformer_d2s.TransformPlan(*query_plan);

	string serialized;
	transformer_d2s.SerializeToString(serialized);

	substrait::Plan splan2;
	SubstraitToDuckDB transformer_s2d(con, splan2);
	auto duckdb_rel = transformer_s2d.TransformPlan(splan2);
	splan2.Clear();
	auto round_trip_result = duckdb_rel->Execute();
	auto correct = round_trip_result->Equals(*actual_result);
	assert(correct);
}

int main() {
	GOOGLE_PROTOBUF_VERIFY_VERSION;
	duckdb::DuckDB db;

	duckdb::Connection con(db);
	con.BeginTransaction(); // somehow we need this
	                        // create TPC-H tables and data

	con.Query("call dbgen(sf=0.1)");

	con.Query("CREATE TABLE person (name text,money int);");
	con.Query("insert into person values ('Pedro', 10);");

	roundtrip_query(con, "select * from person");

//	roundtrip_tpch_plan(con, 1);
	// roundtrip_tpch_plan(con, 2);// delim
	// join
//	roundtrip_tpch_plan(con, 3);
	//  roundtrip_tpch_plan(con, 4); // SEMI join not supported in Substrait
//	roundtrip_tpch_plan(con, 5);
//	roundtrip_tpch_plan(con, 6);
//	roundtrip_tpch_plan(con, 7);
//	roundtrip_tpch_plan(con, 8);
//	roundtrip_tpch_plan(con, 9);
//	roundtrip_tpch_plan(con, 10);
//	roundtrip_tpch_plan(con, 11);
//	roundtrip_tpch_plan(con, 12);
//	roundtrip_tpch_plan(con, 13);
//	roundtrip_tpch_plan(con, 14);
	// roundtrip_tpch_plan(con, 15); // no name on min/max

	// roundtrip_tpch_plan(con, 19); // ??

	//	// transform_plan(con, duckdb::TPCHExtension::GetQuery(16)); // mark
	// join
	//	// transform_plan(con, duckdb::TPCHExtension::GetQuery(17)); // delim
	// join
	//	// transform_plan(con, duckdb::TPCHExtension::GetQuery(18)); // hugeint
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(19));
	//	// transform_plan(con, duckdb::TPCHExtension::GetQuery(20)); // delim
	// join
	//	// transform_plan(con, duckdb::TPCHExtension::GetQuery(21)); // delim
	// join
	//	// transform_plan(con, duckdb::TPCHExtension::GetQuery(22)); // mark
	// join

	// TODO CASE -> SwitchExpression
	// TODO translate missing queries
	// TODO optimize all delim joins away for tpch?
	google::protobuf::ShutdownProtobufLibrary();
}
