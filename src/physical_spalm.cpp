#include "physical_spalm.hpp"
#include "duckdb/execution/operator/join/physical_join.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include <dlfcn.h>
#include <iostream>
#include <cmath>

namespace duckdb {
PhysicalSpalm::PhysicalSpalm(vector<LogicalType> types, PhysicalOperator &left, PhysicalOperator &right,
                               unique_ptr<LogicalAggregate> agg_op_prev,
                               unique_ptr<LogicalComparisonJoin> comp_join_op_prev, idx_t lhs_col_num,
                               idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      agg_op(std::move(agg_op_prev)), comp_join_op(std::move(comp_join_op_prev)), lhs_col_num(lhs_col_num) {
	children.push_back(left);
	children.push_back(right);

	// Index of join key column
	// rhs_join_idx = ((BoundReferenceExpression *)comp_join_op->conditions[0].right.get())->index;
	// rhs_agg_idx = ((BoundReferenceExpression *)agg_op->groups[0].get())->index;
	// TODO : Traverse expression tree, and find index of value column.
	rhs_join_idx = 0;
	rhs_agg_idx = 1;
	rhs_val_idx = 2;

	// Index of join key column
	// lhs_join_idx = ((BoundReferenceExpression *)comp_join_op->conditions[0].left.get())->index;
	// lhs_agg_idx = ((BoundReferenceExpression *)agg_op->groups[1].get())->index;
	// TODO : Traverse expression tree, and find index of value column.
	lhs_join_idx = 0;
	lhs_agg_idx = 1;
	lhs_val_idx = 2;

	// printf("lhs: agg: %ld, join: %ld, val: %ld\n", lhs_agg_idx, lhs_join_idx, lhs_val_idx);
	// printf("rhs: agg: %ld, join: %ld, val: %ld\n", rhs_agg_idx, rhs_join_idx, rhs_val_idx);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class PhysicalSpalmGlobalState : public GlobalSinkState {
public:
	explicit PhysicalSpalmGlobalState(ClientContext &context, const PhysicalSpalm &op) {
		Ri = new int[INIT_DIM + 1];
		Rk = new int[INIT_DIM * INIT_DIM];
		Rv = new double[INIT_DIM * INIT_DIM];
		memset(Ri, 0, sizeof(int) * (INIT_DIM + 1));

		Sj = new int[INIT_DIM + 1];
		Sk = new int[INIT_DIM * INIT_DIM];
		Sv = new double[INIT_DIM * INIT_DIM];
		memset(Sj, 0, sizeof(int) * (INIT_DIM + 1));

		result = new double[INIT_DIM * INIT_DIM];
		memset(result, 0, sizeof(double) * INIT_DIM * INIT_DIM);
	}

	~PhysicalSpalmGlobalState() {
		printf("~PhysicalSpalmGlobalState() called\n");
		delete[] Ri;
		delete[] Rk;
		delete[] Rv;
		delete[] Sj;
		delete[] Sk;
		delete[] Sv;
		delete[] result;
	}

	const int INIT_DIM = 30000;

	const int slice_size = 512;
	const int tile_size_agg_dim = 128;

	int cnt = 0;
	int M, K, N;
	bool dim_init = false;

	int *Ri;
	int *Rk;
	double *Rv;

	int *Sj;
	int *Sk;
	double *Sv;

	double *result;
	bool done = false;

	mutex lock;
};

unique_ptr<GlobalSinkState> PhysicalSpalm::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<PhysicalSpalmGlobalState>(context, *this);
}

SinkResultType PhysicalSpalm::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	// auto &sink = input.global_state.Cast<PhysicalSpalmGlobalState>();
	// lock_guard<mutex> client_guard(sink.rhs_lock);
	// sink.rhs_materialized.Append(sink.append_state, chunk);
	auto &sink = input.global_state.Cast<PhysicalSpalmGlobalState>();
	lock_guard<mutex> client_guard(sink.lock);
	chunk.Flatten();
	const idx_t chunk_size = chunk.size();

	const int *join_col_input_ptr = (int *)chunk.data[rhs_join_idx].GetData();
	const int *agg_col_input_ptr = (int *)chunk.data[rhs_agg_idx].GetData();
	const double *val_col_input_ptr = (double *)chunk.data[rhs_val_idx].GetData();

	idx_t i = 0;
	if (!sink.dim_init) {
		sink.M = agg_col_input_ptr[i];
		sink.K = (int)val_col_input_ptr[i];
		i++;
		sink.dim_init = true;
		printf("M: %d, K: %d\n", sink.M, sink.K);
	}
	for (; i < chunk_size; i++) {
		const int agg_idx = agg_col_input_ptr[i];
		const int join_idx = join_col_input_ptr[i];
		const double val = val_col_input_ptr[i];
		sink.Ri[agg_idx + 1]++;
		sink.Rk[sink.cnt] = join_idx;
		sink.Rv[sink.cnt] = val;
		sink.cnt++;

		//printf("JOIN: %d AGG: %d VAL: %f\n", join_idx, agg_idx, val);
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalSpalm::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &sink = input.global_state.Cast<PhysicalSpalmGlobalState>();
	for (int i = 0; i < sink.M; i++) {
		sink.Ri[i + 1] += sink.Ri[i];
	}
	sink.dim_init = false;
	sink.cnt = 0;
	return SinkCombineResultType::FINISHED;
}

class PhysicalSpalmOperatorState : public OperatorState {
public:
	explicit PhysicalSpalmOperatorState() {
	}
};

unique_ptr<OperatorState> PhysicalSpalm::GetOperatorState(ExecutionContext &context) const {
	// auto &sink = sink_state->Cast<PhysicalSpalmGlobalState>();
	return make_uniq<PhysicalSpalmOperatorState>();
}

OperatorResultType PhysicalSpalm::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                           GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	//printf("EXECUTE\n");
	auto &sink = sink_state->Cast<PhysicalSpalmGlobalState>();
	lock_guard<mutex> client_guard(sink.lock);
	input.Flatten();
	const idx_t input_size = input.size();

	const int *join_col_input_ptr = (int *)input.data[lhs_join_idx].GetData();
	const int *agg_col_input_ptr = (int *)input.data[lhs_agg_idx].GetData();
	const double *val_col_input_ptr = (double *)input.data[lhs_val_idx].GetData();

	idx_t i = 0;
	if (!sink.dim_init) {
		sink.N = agg_col_input_ptr[i];
		sink.K = (int)val_col_input_ptr[i];
		i++;
		sink.dim_init = true;
		printf("N: %d, K: %d\n", sink.N, sink.K);
	}
	for (; i < input_size; i++) {
		const int agg_idx = agg_col_input_ptr[i];
		const int join_idx = join_col_input_ptr[i];
		const double val = val_col_input_ptr[i];
		sink.Sj[agg_idx + 1]++;
		sink.Sk[sink.cnt] = join_idx;
		sink.Sv[sink.cnt] = val;
		sink.cnt++;

		//printf("JOIN: %d AGG: %d VAL: %f\n", join_idx, agg_idx, val);
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

OperatorFinalizeResultType PhysicalSpalm::FinalExecute(ExecutionContext &context, DataChunk &output,
                                                        GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	//printf("FINAL EXECUTE\n");
	auto &sink = sink_state->Cast<PhysicalSpalmGlobalState>();
	if (!sink.done) {
		// Edit path to point to spalm.so
		void *handle = dlopen(".../spalm.so", RTLD_LAZY);
		if (!handle) {
			std::cerr << "Cannot open library: " << dlerror() << '\n';
			return OperatorFinalizeResultType::FINISHED;
		}
		void (*spalm_start)(int, int *, int *, void *, int *, int *, void *, const int, const int, const int, const int,
		              const int, const int, const int, bool, void *, int *, int *, void *, const int, const char *) =
		    (void (*)(int, int *, int *, void *, int *, int *, void *, const int, const int, const int, const int,
		              const int, const int, const int, bool, void *, int *, int *, void *, const int, const char *))dlsym(handle,
		                                                                                            "spalm_start");

		for (int i = 0; i < sink.N; i++) {
			sink.Sj[i + 1] += sink.Sj[i];
		}

		spalm_start(1, sink.Ri, sink.Rk, sink.Rv, sink.Sj, sink.Sk, sink.Sv, sink.M, sink.K, sink.N, -1,
		             -1, -1, -1, false, sink.result, NULL, NULL, NULL, 20, "no_fallback");
		sink.done = true;
		sink.cnt = 0;
	}

	const int M = sink.M;
	const int N = sink.N;

	const int vec_size = 2048;

	int result_num = 0;
	int start_row = sink.cnt / N;
	int start_col = sink.cnt % N;

	//std::cout << "SINKCNT" << sink.cnt << std::endl;

	for (int j = start_col; j < N; j++) {
		if (sink.result[start_row * N + j]) {
			result_num++;
		}
	}

	int end_row = -1;
	bool do_more_rows = false;
	if (result_num < vec_size) {
		do_more_rows = true;
		for (int i = start_row + 1; i < M; i++) {
			for (int j = 0; j < N; j++) {
				if (sink.result[i * N + j]) {
					result_num++;
				}
			}
			if (result_num >= vec_size) {
				end_row = i;
				break;
			}
		}
		if (end_row == -1) {
			end_row = M - 1;
		}
	}

	const int output_result_num = std::min(result_num, vec_size);
	output.SetCapacity(output_result_num);
	output.SetCardinality(output_result_num);
	int *output_row = (int *)output.data[1].GetData();
	int *output_col = (int *)output.data[0].GetData();
	double *output_val = (double *)output.data[2].GetData();

	if (!do_more_rows) {
		int output_idx = 0;
		int j;
		int did_break = 0;
		//std::cout << "HERE1" << std::endl;
		for (j = start_col; j < N; j++) {
			if (sink.result[start_row * N + j]) {
				output_row[output_idx] = start_row;
				output_col[output_idx] = j;
				output_val[output_idx] = sink.result[start_row * N + j];
				output_idx++;
			}
			if (output_idx >= vec_size) {
				did_break = 1;
				break;
			}
		}
		sink.cnt += j - start_col + did_break;
	} else {
		//std::cout << "HERE2" << std::endl;
		int output_idx = 0;
		int j;
		for (j = start_col; j < N; j++) {
			if (sink.result[start_row * N + j]) {
				output_row[output_idx] = start_row;
				output_col[output_idx] = j;
				output_val[output_idx] = sink.result[start_row * N + j];
				output_idx++;
			}
		}
		sink.cnt += j - start_col;
		//std::cout << sink.cnt << std::endl;
		//std::cout << "HERE2" << output_idx << std::endl;
		for (int i = start_row + 1; i < end_row; i++) {
			for (j = 0; j < N; j++) {
				if (sink.result[i * N + j]) {
					output_row[output_idx] = i;
					output_col[output_idx] = j;
					output_val[output_idx] = sink.result[i * N + j];
					output_idx++;
				}
			}
		}
		sink.cnt += N * (end_row - (start_row + 1));
		//std::cout << sink.cnt << std::endl;
		//std::cout << "HERE3" << output_idx << std::endl;
		int did_break = 0;
		for (j = 0; j < N; j++) {
			if (sink.result[end_row * N + j]) {
				output_row[output_idx] = end_row;
				output_col[output_idx] = j;
				output_val[output_idx] = sink.result[end_row * N + j];
				output_idx++;
			}
			if (output_idx >= vec_size) {
				did_break = 1;
				break;
			}
		}
		sink.cnt += j + did_break;
		//std::cout << sink.cnt << std::endl;
	}

	if (sink.cnt != sink.M * sink.N) {
		return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
	} else {
		return OperatorFinalizeResultType::FINISHED;
	}





	/*
	int result_num = 0;
	int next_sink_cnt = std::min(sink.cnt + vec_size, sink.M * sink.N);
	for (int i = sink.cnt; i < next_sink_cnt; i++) {
		//printf("%f\n", sink.result[i]);
		if (sink.result[i]) {
			result_num++;
		}
	}

	const int N = sink.N;

	// if (comp_join_op->join_type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
	if (true) {
		output.SetCapacity(result_num);
		output.SetCardinality(result_num);
		int *output_row = (int *)output.data[1].GetData();
		int *output_col = (int *)output.data[0].GetData();
		double *output_val = (double *)output.data[2].GetData();

		int start_row = sink.cnt / N;
		int start_col = sink.cnt % N;
		int end_col = std::min(start_col + result_num, N);

		int output_idx = 0;

		std::memset(output_row, start_row, sizeof(int) * (end_col - start_col));
		for (int j = start_col; j < N; j++) {
			if (sink.result[start_row * N + j]) {
				output_col[output_idx] = j;
				output_val[output_idx] = sink.result[start_row * N + j];
				output_idx++;
			}
		}

		int end_row = next_sink_cnt / N;
		end_col = next_sink_cnt % N ? next_sink_cnt % N : N;

		int row_idx = start_row + 1;
		//printf("%d %d %d\n", next_sink_cnt, row_idx, end_row);
		for (; row_idx < end_row; row_idx++) {
			int end = N;
			if (row_idx == end_row - 1) {
				end = end_col;
			}
			int prev_output_idx = output_idx;
			for (int j = 0; j < end; j++) {
				if (sink.result[row_idx * N + j]) {
					output_row[output_idx] = row_idx;
					output_col[output_idx] = j;
					output_val[output_idx] = sink.result[row_idx * N + j];
					output_idx++;
				}
			}
			std::memset(&output_row[output_idx], row_idx, sizeof(int) * (output_idx - prev_output_idx));
		}
		sink.cnt += std::min(2048, sink.M * sink.N - sink.cnt);
	}

	if (sink.cnt != sink.M * sink.N) {
		return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
	} else {
		return OperatorFinalizeResultType::FINISHED;
	}
	*/
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalSpalm::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	PhysicalJoin::BuildJoinPipelines(current, meta_pipeline, *this);
}

vector<const_reference<PhysicalOperator>> PhysicalSpalm::GetSources() const {
	return children[0].get().GetSources();
}
} // namespace duckdb
