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

#include "plan.pb.h"
#include "relations.pb.h"
#include "expression.pb.h"

#include <string>
#include <fstream>

using namespace std;

namespace substrait = io::substrait;

struct DuckDBPlanToSubstrait {
	substrait::Plan &plan;
	unordered_map<string, uint64_t> functions_map;

	idx_t last_function_id = 0;

	explicit DuckDBPlanToSubstrait(substrait::Plan &plan_p) : plan(plan_p) {
	}

	static void TransformConstant(duckdb::Value &dval, substrait::Expression_Literal &sval) {
		auto &duckdb_type = dval.type();
		switch (duckdb_type.id()) {
		case duckdb::LogicalTypeId::DECIMAL: {
			// TODO use actual decimals
			sval.set_fp64(dval.GetValue<double>());
			break;
		}
		case duckdb::LogicalTypeId::INTEGER: {
			sval.set_i32(dval.GetValue<int32_t>());
			break;
		}
		case duckdb::LogicalTypeId::DATE: {
			// TODO how are we going to represent dates?
			sval.set_string(dval.ToString());
			break;
		}
		case duckdb::LogicalTypeId::VARCHAR: {
			sval.set_string(dval.GetValue<string>());
			break;
		}

		default:
			throw runtime_error(duckdb_type.ToString());
		}
	}

	void TransformExpr(duckdb::Expression &dexpr, substrait::Expression &sexpr, idx_t col_offset = 0) {
		switch (dexpr.type) {
		case duckdb::ExpressionType::BOUND_REF: {
			auto &dref = (duckdb::BoundReferenceExpression &)dexpr;
			// TODO make field ref a function, its used multiple times
			sexpr.mutable_selection()->mutable_direct_reference()->mutable_struct_field()->set_field(
			    (int32_t)dref.index + col_offset);
			return;
		}
		case duckdb::ExpressionType::BOUND_FUNCTION: {
			auto &dfun = (duckdb::BoundFunctionExpression &)dexpr;
			auto sfun = sexpr.mutable_scalar_function();
			sfun->mutable_id()->set_id(RegisterFunction(dfun.function.name));

			for (auto &darg : dfun.children) {
				auto sarg = sfun->add_args();
				TransformExpr(*darg, *sarg, col_offset);
			}

			return;
		}
		case duckdb::ExpressionType::VALUE_CONSTANT: {
			auto &dconst = (duckdb::BoundConstantExpression &)dexpr;
			auto sconst = sexpr.mutable_literal();
			TransformConstant(dconst.value, *sconst);
			return;
		}
		case duckdb::ExpressionType::COMPARE_LESSTHAN: {
			auto &dcomp = (duckdb::BoundComparisonExpression &)dexpr;

			auto scalar_fun = sexpr.mutable_scalar_function();
			scalar_fun->mutable_id()->set_id(RegisterFunction("lessthan"));
			TransformExpr(*dcomp.left, *scalar_fun->add_args(), col_offset);
			TransformExpr(*dcomp.right, *scalar_fun->add_args(), col_offset);

			return;
		}
		case duckdb::ExpressionType::OPERATOR_IS_NOT_NULL: {
			auto &dop = (duckdb::BoundOperatorExpression &)dexpr;

			auto scalar_fun = sexpr.mutable_scalar_function();
			scalar_fun->mutable_id()->set_id(RegisterFunction("is_not_null"));
			TransformExpr(*dop.children[0], *scalar_fun->add_args(), col_offset);

			return;
		}
		default:
			throw runtime_error(duckdb::ExpressionTypeToString(dexpr.type));
		}
	}

	uint64_t RegisterFunction(string name) {
		if (functions_map.find(name) == functions_map.end()) {
			auto function_id = last_function_id++;
			auto sfun = plan.add_mappings()->mutable_function_mapping();
			sfun->mutable_extension_id()->set_id(42);
			sfun->mutable_function_id()->set_id(function_id);
			sfun->set_index(function_id);
			sfun->set_name(name);

			functions_map[name] = function_id;
		}
		return functions_map[name];
	}

	void TransformFilter(idx_t col_idx, duckdb::TableFilter &dfilter, substrait::Expression &sfilter) {
		switch (dfilter.filter_type) {
		case duckdb::TableFilterType::IS_NOT_NULL: {
			auto &is_not_null_filter = (duckdb::IsNotNullFilter &)dfilter;
			auto scalar_fun = sfilter.mutable_scalar_function();
			scalar_fun->mutable_id()->set_id(RegisterFunction("is_not_null"));
			scalar_fun->add_args()->mutable_selection()->mutable_direct_reference()->mutable_struct_field()->set_field(
			    (int32_t)col_idx);

			return;
		}

		case duckdb::TableFilterType::CONJUNCTION_AND: {
			auto &conjunction_filter = (duckdb::ConjunctionAndFilter &)dfilter;

			// TODO simplify this mess
			substrait::Expression *conjunction_expression = nullptr;
			for (auto &child_filter : conjunction_filter.child_filters) {
				auto child_expression = new substrait::Expression();
				TransformFilter(col_idx, *child_filter, *child_expression);
				if (!conjunction_expression) {
					conjunction_expression = child_expression;
				} else {
					auto temp_expr = new substrait::Expression();
					auto scalar_fun = temp_expr->mutable_scalar_function();
					scalar_fun->mutable_id()->set_id(RegisterFunction("and"));
					scalar_fun->mutable_args()->AddAllocated(conjunction_expression);
					scalar_fun->mutable_args()->AddAllocated(child_expression);
					conjunction_expression = temp_expr;
				}
			}
			sfilter = *conjunction_expression;

			return;
		}
		case duckdb::TableFilterType::CONSTANT_COMPARISON: {
			auto &constant_filter = (duckdb::ConstantFilter &)dfilter;
			sfilter.mutable_scalar_function()
			    ->add_args()
			    ->mutable_selection()
			    ->mutable_direct_reference()
			    ->mutable_struct_field()
			    ->set_field((int32_t)col_idx);
			TransformConstant(constant_filter.constant,
			                  *sfilter.mutable_scalar_function()->add_args()->mutable_literal());

			uint64_t function_id;
			switch (constant_filter.comparison_type) {
			case duckdb::ExpressionType::COMPARE_EQUAL:
				function_id = RegisterFunction("equal");
				break;
			case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
				function_id = RegisterFunction("lessthanequal");
				break;
			case duckdb::ExpressionType::COMPARE_LESSTHAN:
				function_id = RegisterFunction("lessthan");
				break;
			case duckdb::ExpressionType::COMPARE_GREATERTHAN:
				function_id = RegisterFunction("greaterthan");
				break;
			case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
				function_id = RegisterFunction("greaterthanequal");
				break;
			default:
				throw runtime_error(duckdb::ExpressionTypeToString(constant_filter.comparison_type));
			}

			sfilter.mutable_scalar_function()->mutable_id()->set_id(function_id);
			return;
		}
		default:
			throw runtime_error("Unsupported table filter type");
		}
	}

	void TransformJoinCond(duckdb::JoinCondition &dcond, substrait::Expression &scond, idx_t left_ncol) {
		string join_comparision;
		switch (dcond.comparison) {
		case duckdb::ExpressionType::COMPARE_EQUAL:
			join_comparision = "equal";
			break;
		default:
			throw runtime_error("Unsupported join comparision");
		}
		auto scalar_fun = scond.mutable_scalar_function();
		scalar_fun->mutable_id()->set_id(RegisterFunction(join_comparision));
		TransformExpr(*dcond.left, *scalar_fun->add_args());
		TransformExpr(*dcond.right, *scalar_fun->add_args(), left_ncol);
	}

	void TransformOrder(duckdb::BoundOrderByNode &dordf, substrait::Expression_SortField &sordf) {
		switch (dordf.type) {
		case duckdb::OrderType::ASCENDING:
			switch (dordf.null_order) {
			case duckdb::OrderByNullType::NULLS_FIRST:
				sordf.set_formal(
				    substrait::Expression_SortField_SortType::Expression_SortField_SortType_ASC_NULLS_FIRST);
				break;
			case duckdb::OrderByNullType::NULLS_LAST:
				sordf.set_formal(
				    substrait::Expression_SortField_SortType::Expression_SortField_SortType_ASC_NULLS_LAST);

				break;
			default:
				throw runtime_error("Unsupported ordering type");
			}
			break;
		case duckdb::OrderType::DESCENDING:
			switch (dordf.null_order) {
			case duckdb::OrderByNullType::NULLS_FIRST:
				sordf.set_formal(
				    substrait::Expression_SortField_SortType::Expression_SortField_SortType_DESC_NULLS_FIRST);
				break;
			case duckdb::OrderByNullType::NULLS_LAST:
				sordf.set_formal(
				    substrait::Expression_SortField_SortType::Expression_SortField_SortType_DESC_NULLS_LAST);

				break;
			default:
				throw runtime_error("Unsupported ordering type");
			}
			break;
		default:
			throw runtime_error("Unsupported ordering type");
		}
		TransformExpr(*dordf.expression, *sordf.mutable_expr());
	}

	void TransformOp(duckdb::LogicalOperator &dop, substrait::Rel &sop) {
		switch (dop.type) {

		case duckdb::LogicalOperatorType::LOGICAL_FILTER: {
			auto &dfilter = (duckdb::LogicalFilter &)dop;
			auto sfilter = sop.mutable_filter();

			TransformOp(*dop.children[0], *sfilter->mutable_input());

			// another one TODO
			substrait::Expression *conjunction_expression = nullptr;

			// TODO yet another instance of this mess, really needs genericification
			for (auto &dcond : dfilter.expressions) {
				auto child_expression = new substrait::Expression();

				TransformExpr(*dcond, *child_expression);

				if (!conjunction_expression) {
					conjunction_expression = child_expression;
				} else {
					auto temp_expr = new substrait::Expression();
					auto scalar_fun = temp_expr->mutable_scalar_function();
					scalar_fun->mutable_id()->set_id(RegisterFunction("and"));
					scalar_fun->mutable_args()->AddAllocated(conjunction_expression);
					scalar_fun->mutable_args()->AddAllocated(child_expression);
					conjunction_expression = temp_expr;
				}
			}
			sfilter->set_allocated_condition(conjunction_expression);

			return;
		}
		case duckdb::LogicalOperatorType::LOGICAL_TOP_N: {
			auto &dtopn = (duckdb::LogicalTopN &)dop;
			auto stopn = sop.mutable_fetch();

			TransformOp(*dop.children[0], *stopn->mutable_input());

			stopn->set_offset(dtopn.offset);
			stopn->set_count(dtopn.limit);
			return;
		}

		case duckdb::LogicalOperatorType::LOGICAL_LIMIT: {
			auto &dtopn = (duckdb::LogicalLimit &)dop;
			auto stopn = sop.mutable_fetch();

			TransformOp(*dop.children[0], *stopn->mutable_input());

			stopn->set_offset(dtopn.offset_val);
			stopn->set_count(dtopn.limit_val);
			return;
		}

		case duckdb::LogicalOperatorType::LOGICAL_ORDER_BY: {
			auto &dord = (duckdb::LogicalOrder &)dop;
			auto sord = sop.mutable_sort();

			TransformOp(*dop.children[0], *sord->mutable_input());

			for (auto &dordf : dord.orders) {
				auto sordf = sord->add_sorts();
				TransformOrder(dordf, *sordf);
			}
			return;
		}

		case duckdb::LogicalOperatorType::LOGICAL_PROJECTION: {
			auto &dproj = (duckdb::LogicalProjection &)dop;
			auto sproj = sop.mutable_project();

			TransformOp(*dop.children[0], *sproj->mutable_input());

			for (auto &dexpr : dproj.expressions) {
				auto sexpr = sproj->add_expressions();
				TransformExpr(*dexpr, *sexpr);
			}
			return;
		}

		case duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
			auto &djoin = (duckdb::LogicalComparisonJoin &)dop;

			auto sjoin_rel = new substrait::Rel();
			auto sjoin = sjoin_rel->mutable_join();

			TransformOp(*dop.children[0], *sjoin->mutable_left());
			TransformOp(*dop.children[1], *sjoin->mutable_right());

			substrait::Expression *conjunction_expression = nullptr;

			auto left_col_count = dop.children[0]->types.size();

			// TODO yet another instance of this mess, really needs genericification
			for (auto &dcond : djoin.conditions) {
				auto child_expression = new substrait::Expression();

				TransformJoinCond(dcond, *child_expression, left_col_count);

				if (!conjunction_expression) {
					conjunction_expression = child_expression;
				} else {
					auto temp_expr = new substrait::Expression();
					auto scalar_fun = temp_expr->mutable_scalar_function();
					scalar_fun->mutable_id()->set_id(RegisterFunction("and"));
					scalar_fun->mutable_args()->AddAllocated(conjunction_expression);
					scalar_fun->mutable_args()->AddAllocated(child_expression);
					conjunction_expression = temp_expr;
				}
			}
			sjoin->set_allocated_expression(conjunction_expression);

			switch (djoin.join_type) {
			case duckdb::JoinType::INNER:
				sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_INNER);
				break;
			default:
				throw runtime_error("Unsupported join type");
			}

			if (djoin.left_projection_map.empty()) {
				for (idx_t i = 0; i < dop.children[0]->types.size(); i++) {
					djoin.left_projection_map.push_back(i);
				}
			}
			if (djoin.right_projection_map.empty()) {
				for (idx_t i = 0; i < dop.children[1]->types.size(); i++) {
					djoin.right_projection_map.push_back(i);
				}
			}
			// TODO uugly
			for (auto left_idx : djoin.left_projection_map) {
				sop.mutable_project()
				    ->add_expressions()
				    ->mutable_selection()
				    ->mutable_direct_reference()
				    ->mutable_struct_field()
				    ->set_field(left_idx);
			}

			for (auto right_idx : djoin.right_projection_map) {
				sop.mutable_project()
				    ->add_expressions()
				    ->mutable_selection()
				    ->mutable_direct_reference()
				    ->mutable_struct_field()
				    ->set_field(right_idx + left_col_count);
			}

			sop.mutable_project()->set_allocated_input(sjoin_rel);

			return;
		}

		case duckdb::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
			auto &daggr = (duckdb::LogicalAggregate &)dop;
			auto saggr = sop.mutable_aggregate();
			TransformOp(*dop.children[0], *saggr->mutable_input());

			// we only do a single grouping set for now
			auto sgrp = saggr->add_groupings();
			for (auto &dgrp : daggr.groups) {
				if (dgrp->type != duckdb::ExpressionType::BOUND_REF) {
					// TODO push projection or push substrait to allow expressions here
					throw runtime_error("No expressions in groupings yet");
				}
				auto &dref = (duckdb::BoundReferenceExpression &)*dgrp;
				sgrp->add_input_fields((int32_t)dref.index);
			}
			for (auto &dmeas : daggr.expressions) {
				auto smeas = saggr->add_measures()->mutable_measure();
				if (dmeas->type != duckdb::ExpressionType::BOUND_AGGREGATE) {
					// TODO push projection or push substrait, too
					throw runtime_error("No non-aggregate expressions in measures yet");
				}
				auto &daexpr = (duckdb::BoundAggregateExpression &)*dmeas;
				smeas->mutable_id()->set_id(RegisterFunction(daexpr.function.name));

				for (auto &darg : daexpr.children) {
					auto sarg = smeas->add_args();
					TransformExpr(*darg, *sarg);
				}
			}
			return;
		}

		case duckdb::LogicalOperatorType::LOGICAL_GET: {
			auto &dget = (duckdb::LogicalGet &)dop;
			auto &table_scan_bind_data = (duckdb::TableScanBindData &)*dget.bind_data;
			auto sget = sop.mutable_read();

			// TODO this is duplicated make this a template?
			substrait::Expression *conjunction_expression = nullptr;

			for (auto &filter_entry : dget.table_filters.filters) {
				auto col_idx = filter_entry.first;
				auto &filter = *filter_entry.second;
				auto child_expression = new substrait::Expression();

				TransformFilter(col_idx, filter, *child_expression);

				if (!conjunction_expression) {
					conjunction_expression = child_expression;
				} else {
					auto temp_expr = new substrait::Expression();
					auto scalar_fun = temp_expr->mutable_scalar_function();
					scalar_fun->mutable_id()->set_id(RegisterFunction("and"));
					scalar_fun->mutable_args()->AddAllocated(conjunction_expression);
					scalar_fun->mutable_args()->AddAllocated(child_expression);
					conjunction_expression = temp_expr;
				}
			}
			if (conjunction_expression) {
				sget->set_allocated_filter(conjunction_expression);
			}

			for (auto column_index : dget.column_ids) {
				sget->mutable_projection()->mutable_select()->add_struct_items()->set_field((int32_t)column_index);
			}

			// TODO add schema
			sget->mutable_named_table()->add_names(table_scan_bind_data.table->name);
			sget->mutable_common()->mutable_direct();

			return;
		}

		default:
			throw runtime_error(duckdb::LogicalOperatorToString(dop.type));
		}
	}
};

struct SubstraitPlanToDuckDB {
	duckdb::Connection &con;
	substrait::Plan &plan;

	unordered_map<uint64_t, string> functions_map;

	SubstraitPlanToDuckDB(duckdb::Connection &con_p, substrait::Plan &plan_p) : con(con_p), plan(plan_p) {
		for (auto &smap : plan.mappings()) {
			if (!smap.has_function_mapping()) {
				continue;
			}
			auto &sfmap = smap.function_mapping();
			functions_map[sfmap.function_id().id()] = sfmap.name();
		}
	}

	unique_ptr<duckdb::ParsedExpression> TransformExpr(const substrait::Expression &sexpr) {
		switch (sexpr.rex_type_case()) {
		case substrait::Expression::RexTypeCase::kLiteral: {
			auto slit = sexpr.literal();
			switch (slit.literal_type_case()) {
			case substrait::Expression_Literal::LiteralTypeCase::kFp64: {
				// TODO
				return duckdb::make_unique<duckdb::ConstantExpression>(duckdb::Value::DOUBLE(slit.fp64()));
			}
			case substrait::Expression_Literal::LiteralTypeCase::kString: {
				// TODO lets not construct the expression everywhere ay
				return duckdb::make_unique<duckdb::ConstantExpression>(duckdb::Value(slit.string()));
			}
			case substrait::Expression_Literal::LiteralTypeCase::kI32: {
				// TODO lets not construct the expression everywhere ay
				return duckdb::make_unique<duckdb::ConstantExpression>(duckdb::Value::INTEGER(slit.i32()));
			}
			default:
				throw runtime_error(to_string(slit.literal_type_case()));
			}
		}
		case substrait::Expression::RexTypeCase::kSelection: {
			if (!sexpr.selection().has_direct_reference() || !sexpr.selection().direct_reference().has_struct_field()) {
				throw runtime_error("Can only have direct struct references in selections");
			}
			return duckdb::make_unique<duckdb::PositionalReferenceExpression>(
			    sexpr.selection().direct_reference().struct_field().field() + 1);
		}

		case substrait::Expression::RexTypeCase::kScalarFunction: {
			vector<unique_ptr<duckdb::ParsedExpression>> children;
			for (auto &sarg : sexpr.scalar_function().args()) {
				children.push_back(TransformExpr(sarg));
			}
			auto function_name = FindFunction(sexpr.scalar_function().id().id());
			// string compare galore
			// TODO simplify this
			if (function_name == "and") {
				return duckdb::make_unique<duckdb::ConjunctionExpression>(duckdb::ExpressionType::CONJUNCTION_AND,
				                                                          move(children));
			}
			if (function_name == "lessthan") {
				return duckdb::make_unique<duckdb::ComparisonExpression>(duckdb::ExpressionType::COMPARE_LESSTHAN,
				                                                         move(children[0]), move(children[1]));
			}
			if (function_name == "equal") {
				return duckdb::make_unique<duckdb::ComparisonExpression>(duckdb::ExpressionType::COMPARE_EQUAL,
				                                                         move(children[0]), move(children[1]));
			}
			if (function_name == "lessthanequal") {
				return duckdb::make_unique<duckdb::ComparisonExpression>(
				    duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO, move(children[0]), move(children[1]));
			}
			if (function_name == "greaterthanequal") {
				return duckdb::make_unique<duckdb::ComparisonExpression>(
				    duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO, move(children[0]), move(children[1]));
			}
			if (function_name == "greaterthan") {
				return duckdb::make_unique<duckdb::ComparisonExpression>(duckdb::ExpressionType::COMPARE_GREATERTHAN,
				                                                         move(children[0]), move(children[1]));
			}
			if (function_name == "is_not_null") {
				return duckdb::make_unique<duckdb::OperatorExpression>(duckdb::ExpressionType::OPERATOR_IS_NOT_NULL,
				                                                       move(children[0]));
			}
			return duckdb::make_unique<duckdb::FunctionExpression>(function_name, move(children));
		}
		default:
			throw runtime_error("Unsupported expression type " + to_string(sexpr.rex_type_case()));
		}
	}

	string FindFunction(uint64_t id) {
		if (functions_map.find(id) == functions_map.end()) {
			throw runtime_error("Could not find aggregate function " + to_string(id));
		}
		return functions_map[id];
	}

	duckdb::OrderByNode TransformOrder(const substrait::Expression_SortField &sordf) {

		duckdb::OrderType dordertype;
		duckdb::OrderByNullType dnullorder;

		switch (sordf.formal()) {
		case substrait::Expression_SortField_SortType::Expression_SortField_SortType_ASC_NULLS_FIRST:
			dordertype = duckdb::OrderType::ASCENDING;
			dnullorder = duckdb::OrderByNullType::NULLS_FIRST;
			break;
		case substrait::Expression_SortField_SortType::Expression_SortField_SortType_ASC_NULLS_LAST:
			dordertype = duckdb::OrderType::ASCENDING;
			dnullorder = duckdb::OrderByNullType::NULLS_LAST;
			break;
		case substrait::Expression_SortField_SortType::Expression_SortField_SortType_DESC_NULLS_FIRST:
			dordertype = duckdb::OrderType::DESCENDING;
			dnullorder = duckdb::OrderByNullType::NULLS_FIRST;
			break;
		case substrait::Expression_SortField_SortType::Expression_SortField_SortType_DESC_NULLS_LAST:
			dordertype = duckdb::OrderType::DESCENDING;
			dnullorder = duckdb::OrderByNullType::NULLS_LAST;
			break;
		default:
			throw runtime_error("Unsupported ordering " + to_string(sordf.formal()));
		}

		return {dordertype, dnullorder, TransformExpr(sordf.expr())};
	}

	shared_ptr<duckdb::Relation> TransformOp(const substrait::Rel &sop) {
		switch (sop.RelType_case()) {
		case substrait::Rel::RelTypeCase::kJoin: {
			auto &sjoin = sop.join();

			duckdb::JoinType djointype;
			switch (sjoin.type()) {
			case substrait::JoinRel::JoinType::JoinRel_JoinType_INNER:
				djointype = duckdb::JoinType::INNER;
				break;
			default:
				throw runtime_error("Unsupported join type");
			}
			return duckdb::make_shared<duckdb::JoinRelation>(TransformOp(sjoin.left()), TransformOp(sjoin.right()),
			                                                 TransformExpr(sjoin.expression()), djointype);
		}
		case substrait::Rel::RelTypeCase::kFetch: {
			auto &slimit = sop.fetch();
			return duckdb::make_shared<duckdb::LimitRelation>(TransformOp(slimit.input()), slimit.count(),
			                                                  slimit.offset());
		}
		case substrait::Rel::RelTypeCase::kProject: {
			vector<unique_ptr<duckdb::ParsedExpression>> expressions;
			vector<string> aliases;
			idx_t expr_idx = 1;
			for (auto &sexpr : sop.project().expressions()) {
				expressions.push_back(TransformExpr(sexpr));
				aliases.push_back("expr_" + to_string(expr_idx++));
			}
			return duckdb::make_shared<duckdb::ProjectionRelation>(TransformOp(sop.project().input()),
			                                                       move(expressions), move(aliases));
		}
		case substrait::Rel::RelTypeCase::kAggregate: {
			vector<unique_ptr<duckdb::ParsedExpression>> groups, expressions;

			if (sop.aggregate().groupings_size() > 1) {
				throw runtime_error("Only single grouping sets are supported for now");
			}
			if (sop.aggregate().groupings_size() > 0) {
				for (auto &input_field : sop.aggregate().groupings(0).input_fields()) {
					groups.push_back(duckdb::make_unique<duckdb::PositionalReferenceExpression>(input_field + 1));
					expressions.push_back(duckdb::make_unique<duckdb::PositionalReferenceExpression>(input_field + 1));
				}
			}

			for (auto &smeas : sop.aggregate().measures()) {
				vector<unique_ptr<duckdb::ParsedExpression>> children;
				for (auto &sarg : smeas.measure().args()) {
					children.push_back(TransformExpr(sarg));
				}
				expressions.push_back(duckdb::make_unique<duckdb::FunctionExpression>(
				    FindFunction(smeas.measure().id().id()), move(children)));
			}

			return duckdb::make_shared<duckdb::AggregateRelation>(TransformOp(sop.aggregate().input()),
			                                                      move(expressions), move(groups));
		}
		case substrait::Rel::RelTypeCase::kRead: {
			auto &sget = sop.read();
			if (!sget.has_named_table()) {
				throw runtime_error("Can only scan named tables for now");
			}

			auto scan = con.Table(sop.read().named_table().names(0));

			if (sget.has_filter()) {
				scan = duckdb::make_shared<duckdb::FilterRelation>(move(scan), TransformExpr(sget.filter()));
			}

			if (sget.has_projection()) {
				vector<unique_ptr<duckdb::ParsedExpression>> expressions;
				vector<string> aliases;
				idx_t expr_idx = 0;
				for (auto &sproj : sget.projection().select().struct_items()) {
					aliases.push_back("expr_" + to_string(expr_idx++));
					// TODO make sure nothing else is in there
					expressions.push_back(
					    duckdb::make_unique<duckdb::PositionalReferenceExpression>(sproj.field() + 1));
				}

				scan = duckdb::make_shared<duckdb::ProjectionRelation>(move(scan), move(expressions), move(aliases));
			}

			return scan;
		}
		case substrait::Rel::RelTypeCase::kSort: {
			vector<duckdb::OrderByNode> order_nodes;
			for (auto &sordf : sop.sort().sorts()) {
				order_nodes.push_back(TransformOrder(sordf));
			}
			return duckdb::make_shared<duckdb::OrderRelation>(TransformOp(sop.sort().input()), move(order_nodes));
		}
		default:
			throw runtime_error("Unsupported relation type " + to_string(sop.RelType_case()));
		}
	}
};

static unique_ptr<duckdb::QueryResult> roundtrip_plan(duckdb::Connection &con, string q) {

	auto dplan = con.context->ExtractPlan(q);

	substrait::Plan splan;
	DuckDBPlanToSubstrait transformer_d2s(splan);

	transformer_d2s.TransformOp(*dplan, *splan.add_relations());

	string serialized;
	if (!splan.SerializeToString(&serialized)) {
		throw runtime_error("eek");
	}

	// readback woo
	substrait::Plan splan2;
	splan2.ParseFromString(serialized);

	SubstraitPlanToDuckDB transformer_s2d(con, splan2);
	auto duckdb_rel = transformer_s2d.TransformOp(splan2.relations(0));

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
	//  roundtrip_tpch_plan(con, 7); // OR
	// roundtrip_tpch_plan(con, 8); // CASE
	// roundtrip_tpch_plan(con, 9); // CAST
	// roundtrip_tpch_plan(con, 10);

	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(7));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(8));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(9));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(10));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(11));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(12));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(13));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(14));
	//	transform_plan(con, duckdb::TPCHExtension::GetQuery(15));
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

	// TODO translate back to duckdb plan, execute back translation, check results
	// vs original plan
	// TODO translate missing queries
	// TODO optimize all delim joins away for tpch?
}
