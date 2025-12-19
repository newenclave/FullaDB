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
option(FULLADB_BUILD_FULLA_FS "Build fulla-fs test project target" ON)

if (FULLADB_BUILD_FULLA_FS)
    
include(FetchContent)
    FetchContent_Declare(
        replxx
        GIT_REPOSITORY https://github.com/AmokHuginnsson/replxx.git
        GIT_TAG        release-0.0.4
    )
    set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(replxx)

    file(GLOB_RECURSE FULLAFS_HEADERS
        "${CMAKE_CURRENT_SOURCE_DIR}/fullafs/include/*.hpp"
    )
    
    add_executable(fullafs
        fullafs/main.cpp
        ${FULLAFS_HEADERS}
    )
    target_link_libraries(fullafs PRIVATE fulladb replxx)  # Link replxx
    target_link_libraries(fullafs PRIVATE fulladb)    
    target_include_directories(fullafs PRIVATE ${FULLA_HEADERS})
    target_include_directories(fullafs PRIVATE fullafs/include)
    
    source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR}/fullafs/include
             PREFIX "include"
             FILES ${FULLAFS_HEADERS})

    add_executable(fullafs_tests
        fullafs_tests/main.cpp
        fullafs_tests/test_fs_page_allocator.cpp
        fullafs_tests/test_fs_root.cpp
    )
    target_link_libraries(fullafs_tests PRIVATE fulladb)    
    target_include_directories(fullafs_tests PRIVATE ${FULLA_HEADERS})
    target_include_directories(fullafs_tests PRIVATE fullafs)
    target_include_directories(fullafs_tests PRIVATE fullafs/include)
    target_compile_definitions(fullafs_tests PRIVATE REPLXX_STATIC)
    target_compile_definitions(fullafs PRIVATE REPLXX_STATIC)

endif()

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
        tests/test_bpt_leaf_model.cpp
        tests/test_bpt_page_allocator.cpp
        tests/test_bpt_page_model.cpp
        tests/test_bpt_create_dictionary.cpp
        tests/test_long_storage.cpp
        tests/test_radix_trie.cpp
    )
    
    target_link_libraries(tests PRIVATE fulladb)    
    target_include_directories(tests PRIVATE ${FULLA_HEADERS})
    target_compile_definitions(tests PRIVATE ENABLE_PRIVATE_TESTS)

    include(CTest)
    enable_testing()
    add_test(NAME fulladb_tests COMMAND tests)
endif()
