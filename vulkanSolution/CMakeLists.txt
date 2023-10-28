cmake_minimum_required(VERSION 3.15)
project(MortgageCalc)

set(CMAKE_CXX_COMPILER clang++)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
#release and debug flags etc
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -pedantic")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -D_LIBCPP_DEBUG2 -fsanitize=undefined -g")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")

add_link_options(-lvulkan -lpthread)

#add all cpp files in /src to the executable in cmakelists
file(GLOB SOURCES "src/*.cpp")
add_executable(MortgageCalc ${SOURCES})

add_custom_target(test
	COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target MortgageCalc
	COMMAND ${CMAKE_CURRENT_BINARY_DIR}/MortgageCalc
	DEPENDS MortgageCalc
)