#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"

#include "tpch-extension.hpp"

#include "compare_result.hpp"

#include "plan.pb.h"

#include "duckdb_to_substrait.hpp"
#include "substrait_to_duckdb.hpp"

#include <string>
#include <fstream>

using namespace std;
namespace substrait = io::substrait;

static unique_ptr<duckdb::QueryResult> roundtrip_plan(duckdb::Connection &con, string q) {

	auto dplan = con.context->ExtractPlan(q);

	substrait::Plan splan;
	DuckDBToSubstrait transformer_d2s(splan);

	transformer_d2s.TransformOp(*dplan, *splan.add_relations());

	string serialized;
	if (!splan.SerializeToString(&serialized)) {
		throw runtime_error("eek");
	}
	splan.Clear();

	// readback woo
	substrait::Plan splan2;
	splan2.ParseFromString(serialized);

	SubstraitToDuckDB transformer_s2d(con, splan2);
	auto duckdb_rel = transformer_s2d.TransformOp(splan2.relations(0));
	splan2.Clear();

	// printf("\n%s\n", dplan->ToString().c_str());

	// splan2.PrintDebugString();

	//	con.Query(q)->Print();
	//
	//	duckdb_rel->Print();
	//	duckdb_rel->Execute()->Print();

	return duckdb_rel->Execute();
}

static void roundtrip_tpch_plan(duckdb::Connection &con, int q_nr) {
	auto q = duckdb::TPCHExtension::GetQuery(q_nr);

	auto res = roundtrip_plan(con, q);

	// check the results
	string file_content;
	getline(ifstream(duckdb::StringUtil::Format("duckdb/extension/tpch/dbgen/answers/sf0.1/q%02d.csv", q_nr)),
	        file_content, '\0');
	string compare_result = duckdb::compare_csv(*res, file_content, true).c_str();
	if (!compare_result.empty()) {
		printf("%s\n", compare_result.c_str());
		throw runtime_error("result compare failure");
	}
}

int main() {
	GOOGLE_PROTOBUF_VERIFY_VERSION;
	duckdb::DuckDB db;

	duckdb::TPCHExtension tpch;
	tpch.Load(db);

	duckdb::Connection con(db);
	con.BeginTransaction(); // somehow we need this
	                        // create TPC-H tables and data
	con.Query("call dbgen(sf=0.1)");

	// roundtrip_plan(con, "SELECT n_name, r_name FROM nation join region ON n_regionkey = r_regionkey limit 10");
	// return 0;
	roundtrip_tpch_plan(con, 1);
	// roundtrip_tpch_plan(con, 2);// delim
	// join
	// roundtrip_tpch_plan(con, 3); ??
	//  roundtrip_tpch_plan(con, 4); // SEMI join not supported in Substrait
	roundtrip_tpch_plan(con, 5);
	roundtrip_tpch_plan(con, 6);
	roundtrip_tpch_plan(con, 7);
	// roundtrip_tpch_plan(con, 8); // CASE
	roundtrip_tpch_plan(con, 9);
	// roundtrip_tpch_plan(con, 10); // ?
	// roundtrip_tpch_plan(con, 11); // ?
	// roundtrip_tpch_plan(con, 12); // CASE
	// roundtrip_tpch_plan(con, 13); // join comparision
	// roundtrip_tpch_plan(con, 14); // CASE
	// roundtrip_tpch_plan(con, 15); // ??

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
