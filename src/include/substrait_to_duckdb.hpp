#pragma once

#include <string>
#include <unordered_map>
#include <memory>

namespace io {
namespace substrait {
class Plan;
class Expression;
class Expression_SortField;
class Rel;
} // namespace substrait
} // namespace io

namespace duckdb {
class ParsedExpression;
class Connection;
struct OrderByNode;
class Relation;
} // namespace duckdb

class SubstraitToDuckDB {
public:
	SubstraitToDuckDB(duckdb::Connection &con_p, io::substrait::Plan &plan_p);
	std::shared_ptr<duckdb::Relation> TransformOp(const io::substrait::Rel &sop);

private:
	std::string FindFunction(uint64_t id);

	std::unique_ptr<duckdb::ParsedExpression> TransformExpr(const io::substrait::Expression &sexpr);
	duckdb::OrderByNode TransformOrder(const io::substrait::Expression_SortField &sordf);

	duckdb::Connection &con;
	io::substrait::Plan &plan;

	std::unordered_map<uint64_t, std::string> functions_map;
};