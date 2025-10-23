#include "logical_spalm.hpp"
#include "physical_spalm.hpp"

namespace duckdb {
LogicalSpalm::LogicalSpalm(unique_ptr<LogicalAggregate> agg_op, unique_ptr<LogicalComparisonJoin> comp_join_op,
                             unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right)
    : agg_op(std::move(agg_op)), comp_join_op(std::move(comp_join_op)) {
	children.push_back(std::move(left));
	children.push_back(std::move(right));
}

unique_ptr<LogicalAggregate> agg_op;
unique_ptr<LogicalComparisonJoin> comp_join_op;

vector<ColumnBinding> LogicalSpalm::GetColumnBindings() {
	D_ASSERT(agg_op->groupings_index != DConstants::INVALID_INDEX || agg_op->grouping_functions.empty());
	vector<ColumnBinding> result;
	result.reserve(agg_op->groups.size() + agg_op->expressions.size() + agg_op->grouping_functions.size());
	for (idx_t i = 0; i < agg_op->groups.size(); i++) {
		result.emplace_back(agg_op->group_index, i);
	}
	for (idx_t i = 0; i < agg_op->expressions.size(); i++) {
		result.emplace_back(agg_op->aggregate_index, i);
	}
	for (idx_t i = 0; i < agg_op->grouping_functions.size(); i++) {
		result.emplace_back(agg_op->groupings_index, i);
	}
	return result;
}

void LogicalSpalm::ResolveTypes() {
	if (children.empty()) {
		throw InternalException("Spalm operator needs a child");
	}
	D_ASSERT(agg_op->groupings_index != DConstants::INVALID_INDEX || agg_op->grouping_functions.empty());
	for (auto &expr : agg_op->groups) {
		types.push_back(expr->return_type);
	}
	// get the chunk types from the projection list
	for (auto &expr : agg_op->expressions) {
		types.push_back(expr->return_type);
	}
	for (idx_t i = 0; i < agg_op->grouping_functions.size(); i++) {
		types.emplace_back(LogicalType::BIGINT);
	}
}

string LogicalSpalm::GetExtensionName() const {
	return "spalm";
}
} // namespace duckdb
