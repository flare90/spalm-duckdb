#define DUCKDB_EXTENSION_MAIN

#include "spalm_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void SpalmScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Spalm " + name.GetString() + " 🐥");
	});
}

inline void SpalmOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Spalm " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto spalm_scalar_function = ScalarFunction("spalm", {LogicalType::VARCHAR}, LogicalType::VARCHAR, SpalmScalarFun);
	loader.RegisterFunction(spalm_scalar_function);

	// Register another scalar function
	auto spalm_openssl_version_scalar_function = ScalarFunction("spalm_openssl_version", {LogicalType::VARCHAR},
	                                                            LogicalType::VARCHAR, SpalmOpenSSLVersionScalarFun);
	loader.RegisterFunction(spalm_openssl_version_scalar_function);
}

void SpalmExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string SpalmExtension::Name() {
	return "spalm";
}

std::string SpalmExtension::Version() const {
#ifdef EXT_VERSION_SPALM
	return EXT_VERSION_SPALM;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(spalm, loader) {
	duckdb::LoadInternal(loader);
}
}
