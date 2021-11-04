#pragma once

#include "expression.pb.h"
#include <string>
#include <unordered_map>

namespace io {
namespace substrait {
class Plan;
class Rel;
} // namespace substrait
} // namespace io

namespace duckdb {
class TableFilter;
class LogicalOperator;
class BoundOrderByNode;
class Value;
class Expression;
class JoinCondition;
} // namespace duckdb

class DuckDBToSubstrait {
public:
	DuckDBToSubstrait(io::substrait::Plan &plan_p) : plan(plan_p) {
	}
	void TransformOp(duckdb::LogicalOperator &dop, io::substrait::Rel &sop);

private:
	uint64_t RegisterFunction(std::string name);
	static void CreateFieldRef(io::substrait::Expression *expr, int32_t col_idx);

	void TransformConstant(duckdb::Value &dval, io::substrait::Expression_Literal &sval);
	void TransformExpr(duckdb::Expression &dexpr, io::substrait::Expression &sexpr, uint64_t col_offset = 0);
	void TransformFilter(uint64_t col_idx, duckdb::TableFilter &dfilter, io::substrait::Expression &sfilter);
	void TransformJoinCond(duckdb::JoinCondition &dcond, io::substrait::Expression &scond, uint64_t left_ncol);
	void TransformOrder(duckdb::BoundOrderByNode &dordf, io::substrait::Expression_SortField &sordf);

	template <typename T, typename Func>
	io::substrait::Expression *CreateConjunction(T &source, Func f) {
		io::substrait::Expression *res = nullptr;
		for (auto &ele : source) {
			auto child_expression = new io::substrait::Expression();
			f(ele, child_expression);
			if (!res) {
				res = child_expression;
			} else {
				auto temp_expr = new io::substrait::Expression();
				auto scalar_fun = temp_expr->mutable_scalar_function();
				scalar_fun->mutable_id()->set_id(RegisterFunction("and"));
				scalar_fun->mutable_args()->AddAllocated(res);
				scalar_fun->mutable_args()->AddAllocated(child_expression);
				res = temp_expr;
			}
		}

		return res;
	};

	io::substrait::Plan &plan;
	std::unordered_map<std::string, uint64_t> functions_map;

	uint64_t last_function_id = 0;
};