#pragma once

#include <string>
#include <unordered_map>
#include <memory>
#include "substrait/plan.pb.h"
#include "substrait/algebra.pb.h"
using namespace std;

namespace duckdb {
class TableFilter;
class LogicalOperator;
struct BoundOrderByNode;
class Value;
class Expression;
struct JoinCondition;
} // namespace duckdb

class DuckDBToSubstrait {
public:
	DuckDBToSubstrait() {};

	~DuckDBToSubstrait() {
		//		plan.GetArena()->Reset();
		plan.Clear();
		//		plan_expressions.clear();

		//		plan_relations.clear();
		//		return;
	}

	void TransformPlan(duckdb::LogicalOperator &dop);

	void SerializeToString(string &serialized) {
		if (!plan.SerializeToString(&serialized)) {
			throw runtime_error("eek");
		}
	}

private:
	uint64_t RegisterFunction(std::string name);
	static void CreateFieldRef(substrait::Expression *expr, uint64_t col_idx);

	void TransformOp(duckdb::LogicalOperator &dop, substrait::Rel &sop);
	static void TransformConstant(duckdb::Value &dval, substrait::Expression_Literal &sval);
	void TransformExpr(duckdb::Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset = 0);
	void TransformFilter(uint64_t col_idx, duckdb::TableFilter &dfilter, substrait::Expression &sfilter,
	                     bool recursive);
	void TransformJoinCond(duckdb::JoinCondition &dcond, substrait::Expression &scond, uint64_t left_ncol,
	                       bool recursive);
	void TransformOrder(duckdb::BoundOrderByNode &dordf, substrait::SortField &sordf);

	template <typename T, typename Func>
	substrait::Expression *CreateConjunction(T &source, Func f, bool recursive) {
		recursive = true;
		unique_ptr<substrait::Expression> res;
		for (auto &ele : source) {
			auto child_expression = make_unique<substrait::Expression>();
			f(ele, child_expression.get(), true);
			if (!res) {
				res = move(child_expression);
			} else {
				auto temp_expr = make_unique<substrait::Expression>();
				auto scalar_fun = temp_expr->mutable_scalar_function();
				scalar_fun->set_function_reference(RegisterFunction("and"));
				scalar_fun->mutable_args()->AddAllocated(res.release());
				scalar_fun->mutable_args()->AddAllocated(child_expression.release());
				res = move(temp_expr);
			}
		}
		if (!recursive) {
			plan_expressions.push_back(move(res));
			return plan_expressions.back().get();
		}
		return res.release();
	}

	vector<unique_ptr<substrait::Expression>> plan_expressions;
	vector<unique_ptr<substrait::Rel>> plan_relations;
	substrait::Plan plan;
	std::unordered_map<std::string, uint64_t> functions_map;
	// holds the substrait expressions

	uint64_t last_function_id = 0;
};
