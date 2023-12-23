#pragma once
#include "record.hpp"
#include "vkDeleters.hpp"
#include <arrow/table.h>
#include <memory>
#include <string>
#include <vector>
#include <vulkan/vulkan.h>
#include <vulkan/vulkan_core.h>
#include <chrono>

const std::tm ASSUMED_START_DATE = {0, 0, 0, 1, 0, 123, 0, 0, -1, 0, nullptr};
// future note to future me, the computing itself with its implementation can probly live in another file / class
class Program
{
      public:
	Program(int argc, char **argv);
	~Program();
	void run();

      private:
	void get_physical_device(void);
	void create_logical_device(void);
	void create_vkInstance(void);
	void create_descriptor_set_layout(void);
	void create_compute_pipeline(void);
	void create_buffer(size_t record_count);
	void load_parquet(void);
	void transform_data(void);
	void copyDataToBufferMemory(void);
	std::vector<char> read_shader_file(const std::string &filename);
	void calculate_next_set(void);
	void copy_from_buffer(void);
	arrow::Result<std::shared_ptr<arrow::Table>> write_output(void);
	void create_command_buffer(void);
	void create_descriptor_pool(void);
	void create_descriptor_set(void);
	void create_sync_objects(void);
	int argc;
	char** argv;
	VkInstance instance;
	VkDevice logical_device;
	VkPhysicalDevice physical_device;
	VkQueue compute_queue;
	VkDescriptorSetLayout descriptor_set_layout;
	VkPipelineLayout pipeline_layout;
	VkPipeline compute_pipeline;
	VkBuffer data_buffer;
	VkDeviceMemory data_buffer_memory;
	std::shared_ptr<arrow::Table> parquet_table;
	std::vector<std::string> guid_vec;
	std::vector<mortgage_record::record> record_vec;
	std::vector<mortgage_record::record> output_vec;
	VkCommandBuffer command_buffer;
	VkCommandPool command_pool;
	VkDescriptorPool descriptor_pool;
	VkDescriptorSet descriptor_set;
	VkFence fence_compute_finish;
};