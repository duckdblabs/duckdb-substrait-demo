#include "substrait_to_duckdb.hpp"

#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/expression/list.hpp"
#include "duckdb/main/relation/join_relation.hpp"
#include "duckdb/main/relation/limit_relation.hpp"
#include "duckdb/main/relation/projection_relation.hpp"
#include "duckdb/main/relation/aggregate_relation.hpp"
#include "duckdb/main/relation/filter_relation.hpp"
#include "duckdb/main/relation/order_relation.hpp"
#include "duckdb/main/connection.hpp"

#include "plan.pb.h"

using namespace std;

namespace substrait = io::substrait;

SubstraitToDuckDB::SubstraitToDuckDB(duckdb::Connection &con_p, substrait::Plan &plan_p) : con(con_p), plan(plan_p) {
	for (auto &smap : plan.mappings()) {
		if (!smap.has_function_mapping()) {
			continue;
		}
		auto &sfmap = smap.function_mapping();
		functions_map[sfmap.function_id().id()] = sfmap.name();
	}
}

unique_ptr<duckdb::ParsedExpression> SubstraitToDuckDB::TransformExpr(const substrait::Expression &sexpr) {
	switch (sexpr.rex_type_case()) {
	case substrait::Expression::RexTypeCase::kLiteral: {
		auto slit = sexpr.literal();
		duckdb::Value dval;
		switch (slit.literal_type_case()) {
		case substrait::Expression_Literal::LiteralTypeCase::kFp64:
			dval = duckdb::Value::DOUBLE(slit.fp64());
			break;

		case substrait::Expression_Literal::LiteralTypeCase::kString:
			dval = duckdb::Value(slit.string());
			break;

		case substrait::Expression_Literal::LiteralTypeCase::kI32:
			dval = duckdb::Value::INTEGER(slit.i32());
			break;

		default:
			throw runtime_error(to_string(slit.literal_type_case()));
		}
		return duckdb::make_unique<duckdb::ConstantExpression>(dval);
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
		if (function_name == "or") {
			return duckdb::make_unique<duckdb::ConjunctionExpression>(duckdb::ExpressionType::CONJUNCTION_OR,
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
			return duckdb::make_unique<duckdb::ComparisonExpression>(duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO,
			                                                         move(children[0]), move(children[1]));
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

string SubstraitToDuckDB::FindFunction(uint64_t id) {
	if (functions_map.find(id) == functions_map.end()) {
		throw runtime_error("Could not find aggregate function " + to_string(id));
	}
	return functions_map[id];
}

duckdb::OrderByNode SubstraitToDuckDB::TransformOrder(const substrait::Expression_SortField &sordf) {

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

shared_ptr<duckdb::Relation> SubstraitToDuckDB::TransformOp(const substrait::Rel &sop) {
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
		return duckdb::make_shared<duckdb::JoinRelation>(TransformOp(sjoin.left())->Alias("left"),
		                                                 TransformOp(sjoin.right())->Alias("right"),
		                                                 TransformExpr(sjoin.expression()), djointype);
	}
	case substrait::Rel::RelTypeCase::kFetch: {
		auto &slimit = sop.fetch();
		return duckdb::make_shared<duckdb::LimitRelation>(TransformOp(slimit.input()), slimit.count(), slimit.offset());
	}
	case substrait::Rel::RelTypeCase::kFilter: {
		auto &sfilter = sop.filter();
		return duckdb::make_shared<duckdb::FilterRelation>(TransformOp(sfilter.input()),
		                                                   TransformExpr(sfilter.condition()));
	}
	case substrait::Rel::RelTypeCase::kProject: {
		vector<unique_ptr<duckdb::ParsedExpression>> expressions;
		vector<string> aliases;
		duckdb::idx_t expr_idx = 1;
		for (auto &sexpr : sop.project().expressions()) {
			expressions.push_back(TransformExpr(sexpr));
			aliases.push_back("expr_" + to_string(expr_idx++));
		}
		return duckdb::make_shared<duckdb::ProjectionRelation>(TransformOp(sop.project().input()), move(expressions),
		                                                       move(aliases));
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

		return duckdb::make_shared<duckdb::AggregateRelation>(TransformOp(sop.aggregate().input()), move(expressions),
		                                                      move(groups));
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
			duckdb::idx_t expr_idx = 0;
			for (auto &sproj : sget.projection().select().struct_items()) {
				aliases.push_back("expr_" + to_string(expr_idx++));
				// TODO make sure nothing else is in there
				expressions.push_back(duckdb::make_unique<duckdb::PositionalReferenceExpression>(sproj.field() + 1));
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
