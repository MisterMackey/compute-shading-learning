#pragma once
#include "record.hpp"
#include <arrow/api.h>
namespace parquet_loading {
	arrow::Status load_parquet_file(const std::string& path, std::shared_ptr<arrow::Table>* table);
	void transform_data_to_arrays(std::shared_ptr<arrow::Table> table, std::vector<std::string>& guids, std::vector<mortgage_record::record>& records);
}