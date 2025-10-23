// spalm_extension.cpp
#define DUCKDB_EXTENSION_MAIN
#include "spalm_extension.hpp"

#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "logical_spalm.hpp"

namespace duckdb {

void SpalmExtension::InjectSpalm(unique_ptr<LogicalOperator> &plan) {
	if (!plan) {
		return;
	}

	if (plan->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		if (plan->children[0]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto agg_op = unique_ptr_cast<LogicalOperator, LogicalAggregate>(std::move(plan));
			auto comparison_join =
			    unique_ptr_cast<LogicalOperator, LogicalComparisonJoin>(std::move(agg_op->children[0]));
			auto spalm = make_uniq<LogicalSpalm>(std::move(agg_op), std::move(comparison_join),
			                                       std::move(comparison_join->children[0]),
			                                       std::move(comparison_join->children[1]));
			plan = std::move(spalm);
		}
	}

	for (auto &child : plan->children) {
		InjectSpalm(child);
	}
}

static void OptimizePlan(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	SpalmExtension::InjectSpalm(plan);
}

void SpalmExtension::Load(DuckDB &db) {
	try {
		printf("Loading Spalm extension...\n");

		auto optimizer_extension = make_uniq<OptimizerExtension>();
		optimizer_extension->optimize_function = OptimizePlan;
		db.instance->config.optimizer_extensions.push_back(std::move(*optimizer_extension));

		printf("Spalm extension loaded successfully\n");
	} catch (const std::exception &e) {
		printf("Error loading Spalm extension: %s\n", e.what());
	}
}

} // namespace duckdb

extern "C" {
DUCKDB_EXTENSION_API void spalm_extension_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::SpalmExtension>();
}

DUCKDB_EXTENSION_API const char *spalm_extension_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}
