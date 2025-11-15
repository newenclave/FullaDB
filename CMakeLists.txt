cmake_minimum_required(VERSION 3.20)

project(FullaDB LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON) 

file(GLOB_RECURSE FULLA_HEADERS
    "${CMAKE_CURRENT_SOURCE_DIR}/fulla/include/*.hpp"
)

add_library(fulladb INTERFACE ${FULLA_HEADERS})

target_include_directories(fulladb
    INTERFACE 
    ${CMAKE_CURRENT_SOURCE_DIR}/fulla/include
)

source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR}/fulla/include
             PREFIX "include"
             FILES ${FULLA_HEADERS})

if (MSVC)
    target_compile_options(fulladb INTERFACE /W4 /permissive-)
else()
    target_compile_options(fulladb INTERFACE -Wall -Wextra -Wpedantic -Wconversion)
endif()

option(FULLADB_BUILD_TESTS "Build tests target" ON)

if (FULLADB_BUILD_TESTS)
    add_executable(tests
        tests/tests.cpp
        tests/test_byteorder.cpp
        tests/test_serializer.cpp
        tests/test_codec.cpp
        tests/test_data_view.cpp
        tests/test_prop.cpp
        tests/test_page_header.cpp
        tests/test_page_ranges.cpp
        tests/test_file_device.cpp
        tests/test_buffer_manager.cpp
        tests/test_bpt_memory.cpp
        tests/test_slot_directory.cpp
        tests/test_page_bpt_header.cpp
        tests/test_bpt_inode_model.cpp
    )
    
    target_link_libraries(tests PRIVATE fulladb)    
    target_include_directories(tests PRIVATE ${FULLA_HEADERS})
    
    include(CTest)
    enable_testing()
    add_test(NAME fulladb_tests COMMAND tests)
endif()
