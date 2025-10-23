//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb-spalm/src/include/logical_spalm.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "physical_spalm.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {
class LogicalSpalm : public LogicalExtensionOperator {
public:
	explicit LogicalSpalm(unique_ptr<LogicalAggregate> agg_op, unique_ptr<LogicalComparisonJoin> comp_join_op,
	                       unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right);
	unique_ptr<LogicalAggregate> agg_op;
	unique_ptr<LogicalComparisonJoin> comp_join_op;

	string GetName() const override {
		return "SPALM";
	}

	string GetExtensionName() const;

public:
	vector<ColumnBinding> GetColumnBindings() override;

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) override {
		D_ASSERT(children.size() == 2);
		idx_t lhs_cardinality = children[0]->EstimateCardinality(context);
		idx_t rhs_cardinality = children[1]->EstimateCardinality(context);
		auto &left = generator.CreatePlan(*children[0]);
		auto &right = generator.CreatePlan(*children[1]);
		left.estimated_cardinality = lhs_cardinality;
		right.estimated_cardinality = rhs_cardinality;

		return generator.Make<PhysicalSpalm>(types, left, right, std::move(agg_op), std::move(comp_join_op),
		                                      children[0]->GetColumnBindings().size(), estimated_cardinality);
	}

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
