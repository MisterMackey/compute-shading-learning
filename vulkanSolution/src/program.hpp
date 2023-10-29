#pragma once
#include "vkDeleters.hpp"
#include <arrow/table.h>
#include <memory>
#include <vector>
#include <vulkan/vulkan.h>
#include <vulkan/vulkan_core.h>

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
	std::vector<char> read_shader_file(const std::string &filename);
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
};