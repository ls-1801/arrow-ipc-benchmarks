cmake_minimum_required(VERSION 3.27)
project(arrow_benchmark)

set(CMAKE_CXX_STANDARD 20)
find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(LibLZMA REQUIRED)
find_package(LZ4 REQUIRED)
find_package(unofficial-brotli REQUIRED)
find_package(argparse REQUIRED)
find_package(fmt REQUIRED)
find_package(folly REQUIRED)
find_package(Boost COMPONENTS system REQUIRED)

add_library(common src/common.cpp)
target_link_libraries(common PUBLIC arrow_static argparse::argparse lzma lz4 unofficial::brotli::brotlienc-static unofficial::brotli::brotlidec-static fmt::fmt Folly::folly)

add_executable(FSSource src/fs_source.cpp)
target_link_libraries(FSSource common)

add_executable(FSSink src/fs_sink.cpp)
target_link_libraries(FSSink common)

add_executable(GRPCSource src/socket_source.cpp)
target_link_libraries(GRPCSource common arrow_flight_static gRPC::grpc++)
add_executable(GRPCSink src/socket_sink.cpp)
target_link_libraries(GRPCSink common arrow_flight_static gRPC::grpc++)

# Chances are
add_library(lightning STATIC IMPORTED GLOBAL lightning/src/client.cc lightning/src/store.cc)
set_target_properties(lightning PROPERTIES IMPORTED_LOCATION ${Lightning_DIR}/lib/liblightning.a)
target_include_directories(lightning INTERFACE ${Lightning_DIR}/inc)

add_executable(MemorySourceLightning src/inmemory_source_lightning.cpp lightning/src/store.cc lightning/src/client.cc)
target_link_libraries(MemorySourceLightning common lightning rt pthread)

add_executable(MemorySinkLightning src/inmemory_sink_lightning.cpp lightning/src/client.cc)
target_link_libraries(MemorySinkLightning common lightning rt pthread)

add_executable(MemorySource src/inmemory_source.cpp)
target_link_libraries(MemorySource common)

add_executable(MemorySink src/inmemory_sink.cpp)
target_link_libraries(MemorySink common)
