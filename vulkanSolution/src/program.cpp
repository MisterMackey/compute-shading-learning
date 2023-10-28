#include "program.hpp"
#include <fstream>
#include <algorithm>
#include <cstdint>
#include <iostream>
#include <memory>
#include <vector>
#include <array>
#include <vulkan/vk_platform.h>
#include <vulkan/vulkan_core.h>
#include "record.hpp"

using namespace mortgage_record;

#ifndef NDEBUG
const bool enable_validation_layer = true;
#else
const bool enable_validation_layer = false;
#endif

const std::vector<const char *> validation_layers = {
    "VK_LAYER_KHRONOS_validation",
};

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
}

Program::~Program()
{
	vkDestroyPipeline(logical_device, compute_pipeline, nullptr);
	vkDestroyPipelineLayout(logical_device, pipeline_layout, nullptr);
	vkDestroyDescriptorSetLayout(logical_device, descriptor_set_layout, nullptr);
	vkDestroyDevice(logical_device, nullptr);
	vkDestroyInstance(instance, nullptr);
}

void Program::run()
{
	// run implementation
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
		//check for float64 support
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
	//enable float64
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
	//Create the compute shader module
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

std::vector<char> Program::read_shader_file(const std::string& file_name)
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