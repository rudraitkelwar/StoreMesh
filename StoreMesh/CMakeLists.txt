cmake_minimum_required(VERSION 3.16)
project(StoreMesh VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ── Source files ──────────────────────────────────────────────────────────────
set(SOURCES
    src/block_device.cpp
    src/storage_node.cpp
    src/replication_manager.cpp
    src/main.cpp
)

add_executable(storemesh ${SOURCES})

target_include_directories(storemesh PRIVATE include)

# ── Platform threading ────────────────────────────────────────────────────────
find_package(Threads REQUIRED)
target_link_libraries(storemesh PRIVATE Threads::Threads)

# ── Compiler warnings ─────────────────────────────────────────────────────────
target_compile_options(storemesh PRIVATE
    -Wall -Wextra -Wpedantic -Wshadow -Wno-unused-parameter
)

# ── Build modes ───────────────────────────────────────────────────────────────
# Debug:   cmake -DCMAKE_BUILD_TYPE=Debug ..
#   → adds ThreadSanitizer to catch data races at runtime
# Release: cmake -DCMAKE_BUILD_TYPE=Release ..
#   → -O3 for benchmarking

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_compile_options(storemesh PRIVATE
        -g -O0 -fsanitize=thread -fno-omit-frame-pointer
    )
    target_link_options(storemesh PRIVATE -fsanitize=thread)
elseif(CMAKE_BUILD_TYPE STREQUAL "Release")
    target_compile_options(storemesh PRIVATE -O3 -DNDEBUG)
endif()
