#include "duckdb_to_substrait.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/operator/list.hpp"

#include "duckdb/function/table/table_scan.hpp"

#include "plan.pb.h"
#include "expression.pb.h"

using namespace std;

namespace substrait = io::substrait;

void DuckDBToSubstrait::TransformConstant(duckdb::Value &dval, substrait::Expression_Literal &sval) {
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

void DuckDBToSubstrait::TransformExpr(duckdb::Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset) {
	switch (dexpr.type) {
	case duckdb::ExpressionType::BOUND_REF: {
		auto &dref = (duckdb::BoundReferenceExpression &)dexpr;
		CreateFieldRef(&sexpr, dref.index + col_offset);
		return;
	}
	case duckdb::ExpressionType::OPERATOR_CAST: {
		auto &dcast = (duckdb::BoundCastExpression &)dexpr;
		auto sfun = sexpr.mutable_scalar_function();
		sfun->mutable_id()->set_id(RegisterFunction("cast"));
		TransformExpr(*dcast.child, *sfun->add_args(), col_offset);
		sfun->add_args()->mutable_literal()->set_string(dcast.return_type.ToString());
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
	case duckdb::ExpressionType::COMPARE_EQUAL:
	case duckdb::ExpressionType::COMPARE_LESSTHAN: {
		auto &dcomp = (duckdb::BoundComparisonExpression &)dexpr;

		string fname;
		switch (dexpr.type) {
		case duckdb::ExpressionType::COMPARE_EQUAL:
			fname = "equal";
			break;
		case duckdb::ExpressionType::COMPARE_LESSTHAN:
			fname = "lessthan";
			break;
		default:
			throw runtime_error(duckdb::ExpressionTypeToString(dexpr.type));
		}

		auto scalar_fun = sexpr.mutable_scalar_function();
		scalar_fun->mutable_id()->set_id(RegisterFunction(fname));
		TransformExpr(*dcomp.left, *scalar_fun->add_args(), col_offset);
		TransformExpr(*dcomp.right, *scalar_fun->add_args(), col_offset);

		return;
	}
	case duckdb::ExpressionType::CONJUNCTION_AND:
	case duckdb::ExpressionType::CONJUNCTION_OR: {
		auto &dconj = (duckdb::BoundConjunctionExpression &)dexpr;
		string fname;
		switch (dexpr.type) {
		case duckdb::ExpressionType::CONJUNCTION_AND:
			fname = "and";
			break;
		case duckdb::ExpressionType::CONJUNCTION_OR:
			fname = "or";
			break;
		default:
			throw runtime_error(duckdb::ExpressionTypeToString(dexpr.type));
		}

		auto scalar_fun = sexpr.mutable_scalar_function();
		scalar_fun->mutable_id()->set_id(RegisterFunction(fname));
		TransformExpr(*dconj.children[0], *scalar_fun->add_args(), col_offset);
		TransformExpr(*dconj.children[1], *scalar_fun->add_args(), col_offset);

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

uint64_t DuckDBToSubstrait::RegisterFunction(string name) {
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

void DuckDBToSubstrait::CreateFieldRef(substrait::Expression *expr, int32_t col_idx) {
	expr->mutable_selection()->mutable_direct_reference()->mutable_struct_field()->set_field((int32_t)col_idx);
}

void DuckDBToSubstrait::TransformFilter(uint64_t col_idx, duckdb::TableFilter &dfilter,
                                        substrait::Expression &sfilter) {
	switch (dfilter.filter_type) {
	case duckdb::TableFilterType::IS_NOT_NULL: {
		auto scalar_fun = sfilter.mutable_scalar_function();
		scalar_fun->mutable_id()->set_id(RegisterFunction("is_not_null"));
		CreateFieldRef(scalar_fun->add_args(), col_idx);
		return;
	}

	case duckdb::TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_filter = (duckdb::ConjunctionAndFilter &)dfilter;

		auto sfilter_conj = CreateConjunction(conjunction_filter.child_filters,
		                                      [&](unique_ptr<duckdb::TableFilter> &in, substrait::Expression *out) {
			                                      TransformFilter(col_idx, *in, *out);
		                                      });
		sfilter = *sfilter_conj;

		return;
	}
	case duckdb::TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = (duckdb::ConstantFilter &)dfilter;
		CreateFieldRef(sfilter.mutable_scalar_function()->add_args(), col_idx);

		TransformConstant(constant_filter.constant, *sfilter.mutable_scalar_function()->add_args()->mutable_literal());

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

void DuckDBToSubstrait::TransformJoinCond(duckdb::JoinCondition &dcond, substrait::Expression &scond,
                                          uint64_t left_ncol) {
	string join_comparision;
	switch (dcond.comparison) {
	case duckdb::ExpressionType::COMPARE_EQUAL:
		join_comparision = "equal";
		break;
	case duckdb::ExpressionType::COMPARE_GREATERTHAN:
		join_comparision = "greatherthan";
		break;
	default:
		throw runtime_error("Unsupported join comparision");
	}
	auto scalar_fun = scond.mutable_scalar_function();
	scalar_fun->mutable_id()->set_id(RegisterFunction(join_comparision));
	TransformExpr(*dcond.left, *scalar_fun->add_args());
	TransformExpr(*dcond.right, *scalar_fun->add_args(), left_ncol);
}

void DuckDBToSubstrait::TransformOrder(duckdb::BoundOrderByNode &dordf, substrait::Expression_SortField &sordf) {
	switch (dordf.type) {
	case duckdb::OrderType::ASCENDING:
		switch (dordf.null_order) {
		case duckdb::OrderByNullType::NULLS_FIRST:
			sordf.set_formal(substrait::Expression_SortField_SortType::Expression_SortField_SortType_ASC_NULLS_FIRST);
			break;
		case duckdb::OrderByNullType::NULLS_LAST:
			sordf.set_formal(substrait::Expression_SortField_SortType::Expression_SortField_SortType_ASC_NULLS_LAST);

			break;
		default:
			throw runtime_error("Unsupported ordering type");
		}
		break;
	case duckdb::OrderType::DESCENDING:
		switch (dordf.null_order) {
		case duckdb::OrderByNullType::NULLS_FIRST:
			sordf.set_formal(substrait::Expression_SortField_SortType::Expression_SortField_SortType_DESC_NULLS_FIRST);
			break;
		case duckdb::OrderByNullType::NULLS_LAST:
			sordf.set_formal(substrait::Expression_SortField_SortType::Expression_SortField_SortType_DESC_NULLS_LAST);

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

void DuckDBToSubstrait::TransformOp(duckdb::LogicalOperator &dop, substrait::Rel &sop) {
	switch (dop.type) {

	case duckdb::LogicalOperatorType::LOGICAL_FILTER: {
		auto &dfilter = (duckdb::LogicalFilter &)dop;
		auto res = new substrait::Rel();

		TransformOp(*dop.children[0], *res);

		if (!dfilter.expressions.empty()) {
			auto filter = new substrait::Rel();
			filter->mutable_filter()->set_allocated_input(res);
			filter->mutable_filter()->set_allocated_condition(
			    CreateConjunction(dfilter.expressions, [&](unique_ptr<duckdb::Expression> &in,
			                                               substrait::Expression *out) { TransformExpr(*in, *out); }));
			res = filter;
		}

		if (!dfilter.projection_map.empty()) {
			auto projection = new substrait::Rel();
			projection->mutable_project()->set_allocated_input(res);
			for (auto col_idx : dfilter.projection_map) {
				CreateFieldRef(projection->mutable_project()->add_expressions(), col_idx);
			}
			res = projection;
		}
		sop = *res;

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

		auto left_col_count = dop.children[0]->types.size();

		sjoin->set_allocated_expression(
		    CreateConjunction(djoin.conditions, [&](duckdb::JoinCondition &in, substrait::Expression *out) {
			    TransformJoinCond(in, *out, left_col_count);
		    }));

		switch (djoin.join_type) {
		case duckdb::JoinType::INNER:
			sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_INNER);
			break;
		case duckdb::JoinType::LEFT:
			sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_LEFT);
			break;
		default:
			throw runtime_error("Unsupported join type");
		}

		// somewhat odd semantics on our side
		if (djoin.left_projection_map.empty()) {
			for (uint64_t i = 0; i < dop.children[0]->types.size(); i++) {
				djoin.left_projection_map.push_back(i);
			}
		}
		if (djoin.right_projection_map.empty()) {
			for (uint64_t i = 0; i < dop.children[1]->types.size(); i++) {
				djoin.right_projection_map.push_back(i);
			}
		}
		for (auto left_idx : djoin.left_projection_map) {
			CreateFieldRef(sop.mutable_project()->add_expressions(), left_idx);
		}

		for (auto right_idx : djoin.right_projection_map) {
			CreateFieldRef(sop.mutable_project()->add_expressions(), right_idx + left_col_count);
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

		if (!dget.table_filters.filters.empty()) {
			sget->set_allocated_filter(CreateConjunction(
			    dget.table_filters.filters,
			    [&](pair<const duckdb::idx_t, unique_ptr<duckdb::TableFilter>> &in, substrait::Expression *out) {
				    auto col_idx = in.first;
				    auto &filter = *in.second;
				    TransformFilter(col_idx, filter, *out);
			    }));
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
