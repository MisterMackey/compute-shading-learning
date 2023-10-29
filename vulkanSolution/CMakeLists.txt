cmake_minimum_required(VERSION 3.16)
project(MortgageCalc)

set(CMAKE_CXX_COMPILER clang++)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
#release and debug flags etc
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -pedantic")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -D_LIBCPP_DEBUG2 -fsanitize=undefined -g")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")

add_link_options(-lvulkan -lpthread)
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

#add glsl shader source files
file(GLOB_RECURSE GLSL_SOURCE_FILES
	"shaders/*.comp"
)

#create compile target for each glsl shader file
foreach(GLSL ${GLSL_SOURCE_FILES})
	get_filename_component(FILE_NAME ${GLSL} NAME)
	set(SPIRV "${PROJECT_BINARY_DIR}/shaders/${FILE_NAME}.spv")
	add_custom_command(
		OUTPUT ${SPIRV}
		COMMAND ${CMAKE_COMMAND} -E make_directory "${PROJECT_BINARY_DIR}/shaders/"
		COMMAND glslangValidator -V ${GLSL} -o ${SPIRV}
		DEPENDS ${GLSL}
	)
	list(APPEND SPIRV_BINARY_FILES ${SPIRV})
endforeach(GLSL)

add_custom_target(Shaders DEPENDS ${SPIRV_BINARY_FILES})

#add all cpp files in /src to the executable in cmakelists
file(GLOB SOURCES "src/*.cpp")
add_executable(MortgageCalc ${SOURCES})
add_dependencies(MortgageCalc Shaders)
target_link_libraries(MortgageCalc PRIVATE Arrow::arrow_shared)
target_link_libraries(MortgageCalc PRIVATE Parquet::parquet_shared)

add_custom_target(test
	COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target MortgageCalc
	COMMAND ${CMAKE_CURRENT_BINARY_DIR}/MortgageCalc
	DEPENDS MortgageCalc
)