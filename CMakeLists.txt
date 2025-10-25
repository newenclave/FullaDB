cmake_minimum_required(VERSION 3.20)
project(fulladb LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_library(fulladb INTERFACE)
target_include_directories(fulladb INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)

#add_executable(page_view_tests tests/page_view_tests.cpp)
#target_link_libraries(page_view_tests PRIVATE dbpages)
