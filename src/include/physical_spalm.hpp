//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb-spalm/src/include/physical_spalm.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

// Our actual physical operator
class PhysicalSpalm : public PhysicalOperator {
public:
	PhysicalSpalm(vector<LogicalType> types, PhysicalOperator &left, PhysicalOperator &right,
	               unique_ptr<LogicalAggregate> agg_op, unique_ptr<LogicalComparisonJoin> comp_join_op,
	               idx_t lhs_col_num, idx_t estimated_cardinality);

	string GetName() const override {
		return "PHYSICAL_SPALM";
	}

	unique_ptr<LogicalAggregate> agg_op;
	unique_ptr<LogicalComparisonJoin> comp_join_op;

public:
	// Operator Interface
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	OrderPreservationType OperatorOrder() const override {
		return OrderPreservationType::NO_ORDER;
	}
	bool ParallelOperator() const override {
		return false;
	}

protected:
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;
	OperatorFinalizeResultType FinalExecute(ExecutionContext &context, DataChunk &chunk, GlobalOperatorState &gstate,
	                                        OperatorState &state) const override;

public:
	// Sink Interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return false;
	}
	bool SinkOrderDependent() const override {
		return false;
	}
	bool RequiresFinalExecute() const override {
		return true;
	}

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
	vector<const_reference<PhysicalOperator>> GetSources() const override;

private:
	idx_t rhs_join_idx;
	idx_t rhs_agg_idx;
	idx_t rhs_val_idx;

	idx_t lhs_join_idx;
	idx_t lhs_agg_idx;
	idx_t lhs_val_idx;

	idx_t lhs_col_num;
};
} // namespace duckdb
