#include <arrow/api.h>
#include <arrow/array/array_binary.h>
#include <arrow/io/file.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/io/api.h>
#include <arrow/table.h>
#include <arrow/type_fwd.h>
#include <chrono>
#include <cmath>
#include <parquet/arrow/reader.h>
#include <parquet/exception.h>
#include <stdexcept>
#include "parquet_loader.hpp"
#include "record.hpp"
#include "program.hpp"

arrow::Status parquet_loading::load_parquet_file(const std::string& path, std::shared_ptr<arrow::Table>* table) {
  arrow::Status status;
  std::shared_ptr<arrow::io::ReadableFile> infile;
  ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(path, arrow::default_memory_pool()));

  std::unique_ptr<parquet::arrow::FileReader> reader;
  PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));

  return reader->ReadTable(table);
}

float calculate_monthly_payments(float interest_rate, int term, float notional, int payment_type)
{
	//interest rate is a percentage, so divide by 100 and then by 12 to get the monthly interest rate
	float r = interest_rate / 100 / 12;
	int month_amount = term * 12;
	switch (payment_type) {
		case 0:
		//annuity, monthly payment is the combined interest and repayment, interest is recalculated in the shader and subtracted to get the repayment
		//below is the formula for calculating the monthly payment of an annuity
		return notional * r * std::pow(1 + r, month_amount) / (std::pow(1 + r, month_amount) - 1);
		case 1:
		//linear, monthly payment is the payment_repayment value and the interest is calculated on the remaining notional in the shader
		return notional * (r / 12);
		case 2:
		//interest only, monthly payment is 0 and the interest is calculated on the remaining notional in the shader
		return 0;
		default:
		throw std::runtime_error("Invalid payment type");
	}
}

void parquet_loading::transform_data_to_arrays(std::shared_ptr<arrow::Table> table, std::vector<std::string>& guids, std::vector<mortgage_record::record>& records)
{
	size_t record_count = table->num_rows();
	guids.resize(record_count);
	records.resize(record_count);
	auto guid_col = std::static_pointer_cast<arrow::StringArray>(table->column(0)->chunk(0));
	//todo: check these datatypes, they seem off but they match the schema that is being printed
	auto notional_col = std::static_pointer_cast<arrow::Int64Array>(table->column(1)->chunk(0));
	auto interest_rate_col = std::static_pointer_cast<arrow::DoubleArray>(table->column(2)->chunk(0));
	auto reset_frequency_col = std::static_pointer_cast<arrow::Int64Array>(table->column(3)->chunk(0));
	auto start_date_col = std::static_pointer_cast<arrow::TimestampArray>(table->column(4)->chunk(0));
	auto term_col = std::static_pointer_cast<arrow::Int64Array>(table->column(5)->chunk(0));
	auto remaining_notional_col = std::static_pointer_cast<arrow::DoubleArray>(table->column(6)->chunk(0));
	auto payment_type_col = std::static_pointer_cast<arrow::StringArray>(table->column(7)->chunk(0));
	auto risk_indicator_col = std::static_pointer_cast<arrow::Int64Array>(table->column(8)->chunk(0));
	auto next_reset_date_col = std::static_pointer_cast<arrow::TimestampArray>(table->column(9)->chunk(0));

	for (size_t i = 0; i < record_count; ++i) {
		//seems that timestamp type is microseconds since epoch
		auto start_date = start_date_col->Value(i);
		auto term = term_col->Value(i);
		auto as_tp = std::chrono::system_clock::time_point(std::chrono::microseconds(start_date));
		time_t as_time_t = std::chrono::system_clock::to_time_t(as_tp);
		//std::gmtime returns a static memory location, so we need to copy it
		std::tm end_date = *std::gmtime(&as_time_t);
		end_date.tm_year += term;
		int total_months = (end_date.tm_year - ASSUMED_START_DATE.tm_year) * 12 + (end_date.tm_mon - ASSUMED_START_DATE.tm_mon);
		auto reset_date = next_reset_date_col->Value(i);
		auto as_tp_reset = std::chrono::system_clock::time_point(std::chrono::microseconds(reset_date));
		time_t as_time_t_reset = std::chrono::system_clock::to_time_t(as_tp_reset);
		std::tm reset_date_tm = *std::gmtime(&as_time_t_reset);
		int months_to_reset = (reset_date_tm.tm_year - ASSUMED_START_DATE.tm_year) * 12 + (reset_date_tm.tm_mon - ASSUMED_START_DATE.tm_mon);

		std::string payment_type_string = payment_type_col->GetString(i);
		int payment_type = [](std::string payment_type_string) {
			if (payment_type_string == "Annuity") {
				return 0;
			}
			else if (payment_type_string == "Linear") {
				return 1;
			}
			else if (payment_type_string == "Bullet") {
				return 2;
			}
			else {
				return -1;
			}
		}(payment_type_string);

		//this is to link the guid to the record, as the shader can't access the guid directly
		// we store an index in the record which can be used to find the guid in the guids array
		guids[i] = guid_col->GetString(i);
		records[i].index = i;
		records[i].interest_rate = interest_rate_col->Value(i);
		records[i].remaining_notional = remaining_notional_col->Value(i);
		records[i].reset_frequency = reset_frequency_col->Value(i);
		records[i].max_date_offset = total_months - 1;
		records[i].next_reset_date_offset = months_to_reset - 1;
		records[i].curr_date_offset = 0;
		records[i].payment_type = payment_type;
		records[i].repayment_payment = 0;
		records[i].interest_payment = 0;
		records[i].write_off = 0;
		records[i].risk_indicator = risk_indicator_col->Value(i);
		records[i].monthly_payment = calculate_monthly_payments(records[i].interest_rate, term, records[i].remaining_notional, payment_type);
	}
}