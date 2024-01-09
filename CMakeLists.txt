cmake_minimum_required(VERSION 3.27)
project(arrow_benchmark)

set(CMAKE_CXX_STANDARD 20)
find_package(Arrow REQUIRED)
find_package(LibLZMA REQUIRED)
find_package(LZ4 REQUIRED)
find_package(unofficial-brotli REQUIRED)
find_package(argparse REQUIRED)
find_package(fmt REQUIRED)

add_library(common src/common.cpp)
target_link_libraries(common PUBLIC arrow_static argparse::argparse lzma lz4 unofficial::brotli::brotlienc-static unofficial::brotli::brotlidec-static fmt::fmt)

add_executable(FSSource src/fs_source.cpp)
target_link_libraries(FSSource common)

add_executable(FSSink src/fs_sink.cpp)
target_link_libraries(FSSink common)

#
#add_executable(GRPCSource)
#add_executable(GRPCSink)
#
#add_executable(MemorySource)
#add_executable(MemorySink)

