#include <iostream>
#include <memory>
#include "program.hpp"

int main(int argc, char** argv) {
    for (int i = 0; i < argc; i++) {
	std::cout << "argv[" << i << "] = " << argv[i] << std::endl;
    }
    std::unique_ptr<Program> program(new Program(argc, argv));
    program->run();
    return 0;
}