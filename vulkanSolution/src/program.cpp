#include "program.hpp"
#include "parquet_loader.hpp"
#include "record.hpp"
#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <vulkan/vk_platform.h>
#include <vulkan/vulkan_core.h>

using namespace mortgage_record;

#ifndef NDEBUG
const bool enable_validation_layer = true;
#else
const bool enable_validation_layer = false;
#endif

const std::vector<const char *> validation_layers = {
    "VK_LAYER_KHRONOS_validation",
};

const std::string PARQUET_FILE_PATH = "../test_data.parquet";
// this controls how many records are kept in the result set before a write is triggered.
// after a write the memory can be reused
const size_t MAX_BYTES_FOR_RESULT_SET = 10l * 1024l * 1024l * 1024l;

Program::Program(int argc, char **argv)
{
	this->argc = argc;
	this->argv = argv;
	if (enable_validation_layer) {
		std::cout << "Validation layers enabled" << std::endl;
	}
	create_vkInstance();
	get_physical_device();
	create_logical_device();
	create_descriptor_set_layout();
	create_compute_pipeline();
	create_command_buffer();
	create_descriptor_pool();
	create_descriptor_set();
	load_parquet();
	create_buffer(parquet_table->num_rows());
	parquet_loading::transform_data_to_arrays(parquet_table, guid_vec, record_vec);
	// free the memory as this table is no longer needed
	parquet_table.reset();
}

Program::~Program()
{
	vkDestroyDescriptorPool(logical_device, descriptor_pool, nullptr);
	vkFreeCommandBuffers(logical_device, command_pool, 1, &command_buffer);
	vkDestroyCommandPool(logical_device, command_pool, nullptr);
	vkFreeMemory(logical_device, data_buffer_memory, nullptr);
	vkDestroyBuffer(logical_device, data_buffer, nullptr);
	vkDestroyPipeline(logical_device, compute_pipeline, nullptr);
	vkDestroyPipelineLayout(logical_device, pipeline_layout, nullptr);
	vkDestroyDescriptorSetLayout(logical_device, descriptor_set_layout, nullptr);
	vkDestroyDevice(logical_device, nullptr);
	vkDestroyInstance(instance, nullptr);
}

void Program::run()
{
	copyDataToBufferMemory();
	// resize the output vector to its maximum allowed size, we can shrink at the end if needed
	//  its resized to the amount of full sets, as each pass through the shader will get a full set of size record_vec.size() * sizeof(record)
	size_t allowed_sets = MAX_BYTES_FOR_RESULT_SET / (sizeof(record) * record_vec.size());
	size_t output_vec_size = allowed_sets * record_vec.size();
	output_vec.resize(output_vec_size);
	// find the maximum date offset in the record vec
	auto max_date_offset =
	    std::max_element(record_vec.begin(), record_vec.end(), [](const record &a, const record &b) { return a.max_date_offset < b.max_date_offset; });
	// max_date_offset->max_date_offset is the maximum date offset in the output vec, we can use this as a bound on the for loop
	if (max_date_offset->max_date_offset < 0) {
		throw std::runtime_error("Max date offset is negative, aborting"); // just in case since we are throwing the sign away
	}
	size_t iteration_count = static_cast<size_t>(max_date_offset->max_date_offset);
	for (size_t i = 0; i < iteration_count; i++) {
		// compute next set, take it out of the buffer, copy it to output_vec.
		calculate_next_set();
		copy_from_buffer();
		// check if we are at the max allowed sets, if so we trigger a write and clear the vector
		if (i == allowed_sets) {
			write_output();
			output_vec.clear();
		}
	}
	write_output();
}

void Program::create_vkInstance()
{
	VkApplicationInfo appInfo = {};
	appInfo.sType = VK_STRUCTURE_TYPE_APPLICATION_INFO;
	appInfo.pApplicationName = "Mortgage Calculator";
	appInfo.applicationVersion = VK_MAKE_VERSION(1, 0, 0);
	appInfo.pEngineName = "No Engine";
	appInfo.engineVersion = VK_MAKE_VERSION(1, 0, 0);
	appInfo.apiVersion = VK_API_VERSION_1_2;

	VkInstanceCreateInfo createInfo = {};
	createInfo.sType = VK_STRUCTURE_TYPE_INSTANCE_CREATE_INFO;
	createInfo.pApplicationInfo = &appInfo;
	if (enable_validation_layer) {
		createInfo.enabledLayerCount = static_cast<uint32_t>(validation_layers.size());
		createInfo.ppEnabledLayerNames = validation_layers.data();
	} else {
		createInfo.enabledLayerCount = 0;
	}

	auto result = vkCreateInstance(&createInfo, nullptr, &instance);
	if (result != VK_SUCCESS) {
		throw std::runtime_error("Failed to create instance");
	}
}

void Program::get_physical_device()
{
	uint32_t device_count = 0;
	vkEnumeratePhysicalDevices(instance, &device_count, nullptr);
	if (device_count == 0) {
		throw std::runtime_error("Failed to find GPUs with Vulkan support");
	}
	std::vector<VkPhysicalDevice> devices(device_count);
	vkEnumeratePhysicalDevices(instance, &device_count, devices.data());
	for (const auto &device : devices) {
		VkPhysicalDeviceProperties deviceProperties;
		vkGetPhysicalDeviceProperties(device, &deviceProperties);
		VkPhysicalDeviceFeatures deviceFeatures = {};
		vkGetPhysicalDeviceFeatures(device, &deviceFeatures);
		std::cout << "Found device: " << deviceProperties.deviceName << " with limits:" << std::endl;
		std::cout << "\tWorkGroupCount: [" << deviceProperties.limits.maxComputeWorkGroupCount[0] << "], ["
			  << deviceProperties.limits.maxComputeWorkGroupCount[1] << "], [" << deviceProperties.limits.maxComputeWorkGroupCount[2] << "]"
			  << std::endl;
		std::cout << "\tWorkGroupSize: [" << deviceProperties.limits.maxComputeWorkGroupSize[0] << "], ["
			  << deviceProperties.limits.maxComputeWorkGroupSize[1] << "], [" << deviceProperties.limits.maxComputeWorkGroupSize[2] << "]"
			  << std::endl;
		std::cout << "\tWorkGroupInvocations: [" << deviceProperties.limits.maxComputeWorkGroupInvocations << "]" << std::endl;
		// check for float64 support
		if (!deviceFeatures.shaderFloat64) {
			throw std::runtime_error("Device does not support float64, aborting");
		}
		// picking the last and only device here cuz im lazy and i only got the one GPU anyway
		physical_device = device;
	}
}
void Program::create_logical_device()
{
	uint32_t queue_family_count;
	vkGetPhysicalDeviceQueueFamilyProperties(physical_device, &queue_family_count, nullptr);
	std::vector<VkQueueFamilyProperties> queueFamilies(queue_family_count);
	vkGetPhysicalDeviceQueueFamilyProperties(physical_device, &queue_family_count, queueFamilies.data());
	auto compute_queue_index =
	    std::find_if(queueFamilies.begin(), queueFamilies.end(), [](const VkQueueFamilyProperties &qfp) { return qfp.queueFlags & VK_QUEUE_COMPUTE_BIT; });
	size_t index = static_cast<size_t>(std::distance(queueFamilies.begin(), compute_queue_index));
	VkDeviceQueueCreateInfo queueCreateInfo = {};
	queueCreateInfo.sType = VK_STRUCTURE_TYPE_DEVICE_QUEUE_CREATE_INFO;
	queueCreateInfo.queueFamilyIndex = index;
	queueCreateInfo.queueCount = 1;
	float queuePriority = 1.0f;
	queueCreateInfo.pQueuePriorities = &queuePriority;
	VkPhysicalDeviceFeatures deviceFeatures = {};
	// enable float64
	deviceFeatures.shaderFloat64 = VK_TRUE;
	VkDeviceCreateInfo createInfo = {};
	createInfo.sType = VK_STRUCTURE_TYPE_DEVICE_CREATE_INFO;
	createInfo.pQueueCreateInfos = &queueCreateInfo;
	createInfo.queueCreateInfoCount = 1;
	createInfo.pEnabledFeatures = &deviceFeatures;
	createInfo.enabledExtensionCount = 0;

	auto result = vkCreateDevice(physical_device, &createInfo, nullptr, &logical_device);

	if (result != VK_SUCCESS) {
		throw std::runtime_error("Failed to create logical device");
	}

	vkGetDeviceQueue(logical_device, index, 0, &compute_queue);
}

void Program::create_compute_pipeline()
{
	// Create the compute shader module
	auto computeShaderCode = read_shader_file("shaders/mortgage.comp.spv");
	VkShaderModuleCreateInfo shader_create_info = {};
	shader_create_info.sType = VK_STRUCTURE_TYPE_SHADER_MODULE_CREATE_INFO;
	shader_create_info.codeSize = computeShaderCode.size();
	shader_create_info.pCode = reinterpret_cast<const uint32_t *>(computeShaderCode.data());
	VkShaderModule shaderModule;
	auto result = vkCreateShaderModule(logical_device, &shader_create_info, nullptr, &shaderModule);
	if (result != VK_SUCCESS) {
		throw std::runtime_error("Failed to create shader module");
	}

	VkPipelineShaderStageCreateInfo shader_stage_create_info = {};
	shader_stage_create_info.sType = VK_STRUCTURE_TYPE_PIPELINE_SHADER_STAGE_CREATE_INFO;
	shader_stage_create_info.stage = VK_SHADER_STAGE_COMPUTE_BIT;
	shader_stage_create_info.module = shaderModule;
	shader_stage_create_info.pName = "main";

	VkPipelineLayoutCreateInfo pipeline_layout_create_info = {};
	pipeline_layout_create_info.sType = VK_STRUCTURE_TYPE_PIPELINE_LAYOUT_CREATE_INFO;
	pipeline_layout_create_info.setLayoutCount = 1;
	pipeline_layout_create_info.pSetLayouts = &descriptor_set_layout;

	result = vkCreatePipelineLayout(logical_device, &pipeline_layout_create_info, nullptr, &pipeline_layout);
	if (result != VK_SUCCESS) {
		throw std::runtime_error("Failed to create pipeline layout");
	}

	VkComputePipelineCreateInfo pipeline_create_info = {};
	pipeline_create_info.sType = VK_STRUCTURE_TYPE_COMPUTE_PIPELINE_CREATE_INFO;
	pipeline_create_info.stage = shader_stage_create_info;
	pipeline_create_info.layout = pipeline_layout;

	result = vkCreateComputePipelines(logical_device, VK_NULL_HANDLE, 1, &pipeline_create_info, nullptr, &compute_pipeline);
	if (result != VK_SUCCESS) {
		throw std::runtime_error("Failed to create compute pipeline");
	}

	vkDestroyShaderModule(logical_device, shaderModule, nullptr);
}

void Program::create_descriptor_set_layout()
{
	std::array<VkDescriptorSetLayoutBinding, 1> layout_bindings{};
	layout_bindings[0].binding = 0;
	layout_bindings[0].descriptorType = VK_DESCRIPTOR_TYPE_STORAGE_BUFFER;
	layout_bindings[0].descriptorCount = 1;
	layout_bindings[0].stageFlags = VK_SHADER_STAGE_COMPUTE_BIT;
	layout_bindings[0].pImmutableSamplers = nullptr;

	VkDescriptorSetLayoutCreateInfo descriptor_set_layout_create_info = {};
	descriptor_set_layout_create_info.sType = VK_STRUCTURE_TYPE_DESCRIPTOR_SET_LAYOUT_CREATE_INFO;
	descriptor_set_layout_create_info.bindingCount = static_cast<uint32_t>(layout_bindings.size());
	descriptor_set_layout_create_info.pBindings = layout_bindings.data();

	auto result = vkCreateDescriptorSetLayout(logical_device, &descriptor_set_layout_create_info, nullptr, &descriptor_set_layout);
	if (result != VK_SUCCESS) {
		throw std::runtime_error("Failed to create descriptor set layout");
	}
}

std::vector<char> Program::read_shader_file(const std::string &file_name)
{
	std::ifstream file(file_name, std::ios::ate | std::ios::binary);
	if (!file.is_open()) {
		throw std::runtime_error("Failed to open shader file");
	}
	size_t fileSize = (size_t)file.tellg();
	std::vector<char> buffer(fileSize);
	file.seekg(0);
	file.read(buffer.data(), fileSize);
	file.close();
	return buffer;
}

void Program::create_buffer(size_t record_count)
{
	// todo: CHECK THIS WHOLE METHOD
	VkBufferCreateInfo buffer_create_info = {};
	buffer_create_info.sType = VK_STRUCTURE_TYPE_BUFFER_CREATE_INFO;
	buffer_create_info.size = sizeof(record) * record_count;
	buffer_create_info.usage = VK_BUFFER_USAGE_STORAGE_BUFFER_BIT;
	buffer_create_info.sharingMode = VK_SHARING_MODE_EXCLUSIVE;
	// not settings flags, wasn't needed in tutorial and results in validation errors so guess leave it empty

	auto result = vkCreateBuffer(logical_device, &buffer_create_info, nullptr, &data_buffer);
	if (result != VK_SUCCESS) {
		throw std::runtime_error("Failed to create buffer");
	}

	VkMemoryRequirements memory_requirements = {};
	memory_requirements.size = sizeof(record) * record_count;
	vkGetBufferMemoryRequirements(logical_device, data_buffer, &memory_requirements);

	VkPhysicalDeviceMemoryProperties memory_properties = {};
	vkGetPhysicalDeviceMemoryProperties(physical_device, &memory_properties);

	VkMemoryAllocateInfo memory_allocate_info = {};
	memory_allocate_info.sType = VK_STRUCTURE_TYPE_MEMORY_ALLOCATE_INFO;
	memory_allocate_info.allocationSize = memory_requirements.size;
	// for loop is probably more readable here?
	//  if not, forgive me for i have sinned
	bool err = true;
	for (uint32_t i = 0; i < memory_properties.memoryTypeCount; i++) {
		bool type_suitable = memory_requirements.memoryTypeBits & (1 << i);
		bool property_suitable =
		    memory_properties.memoryTypes[i].propertyFlags & (VK_MEMORY_PROPERTY_HOST_VISIBLE_BIT | VK_MEMORY_PROPERTY_HOST_COHERENT_BIT);
		if (type_suitable && property_suitable) {
			memory_allocate_info.memoryTypeIndex = i;
			err = false;
			break;
		}
	}
	if (err) {
		throw std::runtime_error("Failed to find suitable memory type");
	}

	result = vkAllocateMemory(logical_device, &memory_allocate_info, nullptr, &data_buffer_memory);
	if (result != VK_SUCCESS) {
		throw std::runtime_error("Failed to allocate buffer memory");
	}

	vkBindBufferMemory(logical_device, data_buffer, data_buffer_memory, 0);
}

void Program::load_parquet()
{
	std::cout << "Loading: " << PARQUET_FILE_PATH << std::endl;
	auto result = parquet_loading::load_parquet_file(PARQUET_FILE_PATH, &parquet_table);
	if (result != arrow::Status::OK()) {
		throw std::runtime_error("Failed to load parquet file");
	}
	std::cout << "loaded " << parquet_table->num_rows() << " records" << std::endl;
	std::cout << parquet_table->schema()->ToString(true) << std::endl;
}

void Program::copyDataToBufferMemory()
{
	void *data;
	vkMapMemory(logical_device, data_buffer_memory, 0, sizeof(record) * record_vec.size(), 0, &data);
	memcpy(data, record_vec.data(), sizeof(record) * record_vec.size());
	vkUnmapMemory(logical_device, data_buffer_memory);
}

void Program::create_command_buffer()
{
	uint32_t queue_family_count;
	vkGetPhysicalDeviceQueueFamilyProperties(physical_device, &queue_family_count, nullptr);
	std::vector<VkQueueFamilyProperties> queueFamilies(queue_family_count);
	vkGetPhysicalDeviceQueueFamilyProperties(physical_device, &queue_family_count, queueFamilies.data());
	auto compute_queue_index =
	    std::find_if(queueFamilies.begin(), queueFamilies.end(), [](const VkQueueFamilyProperties &qfp) { return qfp.queueFlags & VK_QUEUE_COMPUTE_BIT; });
	size_t index = static_cast<size_t>(std::distance(queueFamilies.begin(), compute_queue_index));

	VkCommandPoolCreateInfo command_pool_create_info = {};
	command_pool_create_info.sType = VK_STRUCTURE_TYPE_COMMAND_POOL_CREATE_INFO;
	command_pool_create_info.queueFamilyIndex = index;
	command_pool_create_info.flags = VK_COMMAND_POOL_CREATE_RESET_COMMAND_BUFFER_BIT;

	auto result = vkCreateCommandPool(logical_device, &command_pool_create_info, nullptr, &command_pool);
	if (result != VK_SUCCESS) {
		throw std::runtime_error("Failed to create command pool");
	}

	VkCommandBufferAllocateInfo command_buffer_allocate_info = {};
	command_buffer_allocate_info.sType = VK_STRUCTURE_TYPE_COMMAND_BUFFER_ALLOCATE_INFO;
	command_buffer_allocate_info.commandPool = command_pool;
	command_buffer_allocate_info.level = VK_COMMAND_BUFFER_LEVEL_PRIMARY;
	command_buffer_allocate_info.commandBufferCount = 1;

	result = vkAllocateCommandBuffers(logical_device, &command_buffer_allocate_info, &command_buffer);
	if (result != VK_SUCCESS) {
		throw std::runtime_error("Failed to allocate command buffer");
	}
}

void Program::calculate_next_set()
{
	return;
	VkCommandBufferBeginInfo command_buffer_begin_info = {};
	command_buffer_begin_info.sType = VK_STRUCTURE_TYPE_COMMAND_BUFFER_BEGIN_INFO;
	if (vkBeginCommandBuffer(command_buffer, &command_buffer_begin_info) != VK_SUCCESS) {
		throw std::runtime_error("Failed to begin recording command buffer");
	}
	vkCmdBindPipeline(command_buffer, VK_PIPELINE_BIND_POINT_COMPUTE, compute_pipeline);
	//figure out how to make this descriptor sets again
	vkCmdBindDescriptorSets(command_buffer, VK_PIPELINE_BIND_POINT_COMPUTE, pipeline_layout, 0, 1, &descriptor_set, 0, nullptr);
}

void Program::write_output()
{

}

void Program::copy_from_buffer()
{

}

void Program::create_descriptor_pool()
{
	VkDescriptorPoolSize pool_size = {};
	pool_size.type = VK_DESCRIPTOR_TYPE_STORAGE_BUFFER;
	pool_size.descriptorCount = 1;

	VkDescriptorPoolCreateInfo pool_create_info = {};
	pool_create_info.sType = VK_STRUCTURE_TYPE_DESCRIPTOR_POOL_CREATE_INFO;
	pool_create_info.poolSizeCount = 1;
	pool_create_info.pPoolSizes = &pool_size;
	pool_create_info.maxSets = 1;
	pool_create_info.flags = 0; //this can be set optionally to allow freeing of individual descriptor sets if needed

	auto result = vkCreateDescriptorPool(logical_device, &pool_create_info, nullptr, &descriptor_pool);
	if (result != VK_SUCCESS) {
		throw std::runtime_error("Failed to create descriptor pool");
	}
}

void Program::create_descriptor_set()
{
	VkDescriptorSetAllocateInfo descriptor_set_allocate_info = {};
	descriptor_set_allocate_info.sType = VK_STRUCTURE_TYPE_DESCRIPTOR_SET_ALLOCATE_INFO;
	descriptor_set_allocate_info.descriptorPool = descriptor_pool;
	descriptor_set_allocate_info.descriptorSetCount = 1;
	descriptor_set_allocate_info.pSetLayouts = &descriptor_set_layout;

	auto result = vkAllocateDescriptorSets(logical_device, &descriptor_set_allocate_info, &descriptor_set);
	if (result != VK_SUCCESS) {
		throw std::runtime_error("Failed to allocate descriptor set");
	}
	// does not need to be freed explicitly, freed when descriptor pool is freed

	//at this point the objects are allocated but not configured
	// you would run something like the below for each descriptor set but we only have one
	VkDescriptorBufferInfo buffer_info = {};
	buffer_info.buffer = data_buffer;
	buffer_info.offset = 0;
	buffer_info.range = VK_WHOLE_SIZE;
	VkWriteDescriptorSet descriptor_write = {};
	descriptor_write.sType = VK_STRUCTURE_TYPE_WRITE_DESCRIPTOR_SET;
	descriptor_write.dstSet = descriptor_set;
	descriptor_write.dstBinding = 0;
	descriptor_write.dstArrayElement = 0;
	descriptor_write.descriptorType = VK_DESCRIPTOR_TYPE_STORAGE_BUFFER;
	descriptor_write.descriptorCount = 1;
	descriptor_write.pBufferInfo = &buffer_info;

	vkUpdateDescriptorSets(logical_device, 1, &descriptor_write, 0, nullptr);
}