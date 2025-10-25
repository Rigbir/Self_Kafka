cmake_minimum_required(VERSION 3.10)

project(selfkafka 
    VERSION 1.0.0
    DESCRIPTION "Minimal Kafka-like message broker implementation"
    LANGUAGES CXX
)

# Set C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Include directories
include_directories(include)
include_directories(/usr/local/opt/libpq/include)

# Compiler flags
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG")
endif()

# Collect all source files
file(GLOB_RECURSE SOURCES "src/*.cpp")

# Create static library
add_library(selfkafka STATIC ${SOURCES})

# PostgreSQL configuration
set(CMAKE_PREFIX_PATH "/usr/local/opt/libpq")
find_package(PostgreSQL REQUIRED)

# Link PostgreSQL
target_include_directories(selfkafka PRIVATE ${PostgreSQL_INCLUDE_DIRS})
target_link_libraries(selfkafka ${PostgreSQL_LIBRARIES})

# Set target properties
set_target_properties(selfkafka PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    POSITION_INDEPENDENT_CODE ON
)

# Create executable for examples
add_executable(basic_usage examples/basic_usage.cpp)
target_link_libraries(basic_usage selfkafka)

add_executable(async_demo examples/async_demo.cpp)
target_link_libraries(async_demo selfkafka)

add_executable(metrics_demo examples/metrics_demo.cpp)
target_link_libraries(metrics_demo selfkafka)

add_executable(retention_demo examples/retention_demo.cpp)
target_link_libraries(retention_demo selfkafka)

# Optional: Enable testing
option(BUILD_TESTS "Build tests" OFF)
if(BUILD_TESTS)
    enable_testing()
    # Add test targets here when ready
endif()
