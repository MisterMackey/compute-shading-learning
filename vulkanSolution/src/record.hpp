#pragma  once
#include <array>
#include <vulkan/vulkan_core.h>
namespace mortgage_record {
    struct record {
	alignas(8)double index;
	alignas(4)float interest_rate;
	alignas(4)float remaining_notional;
	alignas(4)float monthly_payment;
	alignas(4)float repayment_payment;
	alignas(4)float interest_payment;
	alignas(4)float write_off;
	alignas(4)int reset_frequency;
	alignas(4)int risk_indicator;
	alignas(4)int curr_date_offset;
	alignas(4)int next_reset_date_offset;
	alignas(4)int max_date_offset;
	alignas(4)int payment_type;

	static VkVertexInputBindingDescription get_binding_description(void) {
	    VkVertexInputBindingDescription binding_description = {};
	    binding_description.binding = 0;
	    binding_description.stride = sizeof(record);
	    binding_description.inputRate = VK_VERTEX_INPUT_RATE_VERTEX;
	    return binding_description;
	}

	static std::array<VkVertexInputAttributeDescription, 13> get_attribute_description(void) {
		std::array<VkVertexInputAttributeDescription, 13> attribute_description = {};

		attribute_description[0].binding = 0;
		attribute_description[0].location = 0;
		attribute_description[0].format = VK_FORMAT_R64_SFLOAT;
		attribute_description[0].offset = offsetof(record, index);

		attribute_description[1].binding = 0;
		attribute_description[1].location = 1;
		attribute_description[1].format = VK_FORMAT_R32_SFLOAT;
		attribute_description[1].offset = offsetof(record, interest_rate);

		attribute_description[2].binding = 0;
		attribute_description[2].location = 2;
		attribute_description[2].format = VK_FORMAT_R32_SFLOAT;
		attribute_description[2].offset = offsetof(record, remaining_notional);

		attribute_description[3].binding = 0;
		attribute_description[3].location = 3;
		attribute_description[3].format = VK_FORMAT_R32_SFLOAT;
		attribute_description[3].offset = offsetof(record, monthly_payment);

		attribute_description[4].binding = 0;
		attribute_description[4].location = 4;
		attribute_description[4].format = VK_FORMAT_R32_SFLOAT;
		attribute_description[4].offset = offsetof(record, repayment_payment);

		attribute_description[5].binding = 0;
		attribute_description[5].location = 5;
		attribute_description[5].format = VK_FORMAT_R32_SFLOAT;
		attribute_description[5].offset = offsetof(record, interest_payment);

		attribute_description[6].binding = 0;
		attribute_description[6].location = 6;
		attribute_description[6].format = VK_FORMAT_R32_SFLOAT;
		attribute_description[6].offset = offsetof(record, write_off);

		attribute_description[7].binding = 0;
		attribute_description[7].location = 7;
		attribute_description[7].format = VK_FORMAT_R32_SINT;
		attribute_description[7].offset = offsetof(record, reset_frequency);

		attribute_description[8].binding = 0;
		attribute_description[8].location = 8;
		attribute_description[8].format = VK_FORMAT_R32_SINT;
		attribute_description[8].offset = offsetof(record, risk_indicator);

		attribute_description[9].binding = 0;
		attribute_description[9].location = 9;
		attribute_description[9].format = VK_FORMAT_R32_SINT;
		attribute_description[9].offset = offsetof(record, curr_date_offset);

		attribute_description[10].binding = 0;
		attribute_description[10].location = 10;
		attribute_description[10].format = VK_FORMAT_R32_SINT;
		attribute_description[10].offset = offsetof(record, next_reset_date_offset);

		attribute_description[11].binding = 0;
		attribute_description[11].location = 11;
		attribute_description[11].format = VK_FORMAT_R32_SINT;
		attribute_description[11].offset = offsetof(record, max_date_offset);

		attribute_description[12].binding = 0;
		attribute_description[12].location = 12;
		attribute_description[12].format = VK_FORMAT_R32_SINT;
		attribute_description[12].offset = offsetof(record, payment_type);

		return attribute_description;
	}
    };
}