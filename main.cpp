#include "duckdb.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/parser/expression/list.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/main/relation/limit_relation.hpp"
#include "duckdb/main/relation/order_relation.hpp"
#include "duckdb/main/relation/projection_relation.hpp"
#include "duckdb/main/relation/aggregate_relation.hpp"
#include "duckdb/main/relation/filter_relation.hpp"
#include "duckdb/main/relation/join_relation.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/common/types/date.hpp"


#include "tpch-extension.hpp"

#include "compare_result.hpp"


#include "relations.pb.h"

#include <string>

using namespace std;

int main() {
	duckdb::DuckDB db;

	duckdb::TPCHExtension tpch;
	tpch.Load(db);

	duckdb::Connection con(db);
	con.BeginTransaction(); // somehow we need this
	// create TPC-H tables and data
	con.Query("call dbgen(sf=0.1)");

	// transform_plan(con, "SELECT avg(l_discount) FROM lineitem");

	// transform_plan(con, duckdb::TPCHExtension::GetQuery(1));
	//	// transform_plan(con, duckdb::TPCHExtension::GetQuery(2)); // delim join
	// transform_plan(con, duckdb::TPCHExtension::GetQuery(3));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(4));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(5));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(6));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(7));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(8));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(9));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(10));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(11));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(12));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(13));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(14));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(15));
	//	// transform_plan(con, duckdb::TPCHExtension::GetQuery(16)); // mark join
	//	// transform_plan(con, duckdb::TPCHExtension::GetQuery(17)); // delim join
	//	// transform_plan(con, duckdb::TPCHExtension::GetQuery(18)); // hugeint
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(19));
	//	// transform_plan(con, duckdb::TPCHExtension::GetQuery(20)); // delim join
	//	// transform_plan(con, duckdb::TPCHExtension::GetQuery(21)); // delim join
	//	// transform_plan(con, duckdb::TPCHExtension::GetQuery(22)); // mark join

	// TODO translate back to duckdb plan, execute back translation, check results vs original plan
	// TODO translate missing queries
	// TODO optimize all delim joins away for tpch?
}
