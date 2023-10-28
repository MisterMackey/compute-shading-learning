#pragma once
#include <vulkan/vulkan.h>

struct vkInstanceDeleter {
    void operator()(VkInstance* instance) {
	if (*instance != VK_NULL_HANDLE) {
	    vkDestroyInstance(*instance, nullptr);
	    delete instance;
	}
    }
};