#pragma once
#include <arrow/api.h>
namespace parquet_loading {
  arrow::Status load_parquet_file(const std::string& path, std::shared_ptr<arrow::Table>* table);
}