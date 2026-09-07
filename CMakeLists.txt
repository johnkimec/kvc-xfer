cmake_minimum_required(VERSION 3.20)
project(kvc_xfer VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(KVC_BUILD_TESTS "Build unit tests" ON)
option(KVC_BUILD_TOOLS "Build benchmark and example binaries" ON)
option(KVC_WERROR "Treat warnings as errors" OFF)
option(KVC_SANITIZE "Build with AddressSanitizer + UndefinedBehaviorSanitizer" OFF)

add_library(kvc
  src/status.cpp
  src/layout.cpp
  src/memory.cpp
  src/transfer.cpp
  src/protocol.cpp
  src/logging.cpp
  src/metrics.cpp
  src/local_transport.cpp
  src/tcp_transport.cpp
  src/engine.cpp
  src/handoff.cpp
)
add_library(kvc::kvc ALIAS kvc)
target_include_directories(kvc PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>)
target_compile_options(kvc PRIVATE -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wno-sign-conversion)
if(KVC_WERROR)
  target_compile_options(kvc PRIVATE -Werror)
endif()
find_package(Threads REQUIRED)
target_link_libraries(kvc PUBLIC Threads::Threads)

if(KVC_SANITIZE)
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
endif()

if(KVC_BUILD_TOOLS)
  add_executable(kvc_bench tools/kvc_bench.cpp)
  target_link_libraries(kvc_bench PRIVATE kvc)
  add_executable(pd_demo examples/pd_demo.cpp)
  target_link_libraries(pd_demo PRIVATE kvc)
endif()

if(KVC_BUILD_TESTS)
  enable_testing()
  find_package(GTest CONFIG QUIET)
  if(NOT GTest_FOUND)
    include(FetchContent)
    FetchContent_Declare(googletest
      URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.tar.gz)
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
  endif()
  add_executable(kvc_tests
    tests/test_layout.cpp
    tests/test_protocol.cpp
    tests/test_memory.cpp
    tests/test_transfer.cpp
    tests/test_local_transport.cpp
    tests/test_tcp_transport.cpp
    tests/test_engine.cpp
    tests/test_handoff.cpp
  )
  target_link_libraries(kvc_tests PRIVATE kvc GTest::gtest_main)
  include(GoogleTest)
  gtest_discover_tests(kvc_tests DISCOVERY_TIMEOUT 30)
endif()

include(GNUInstallDirs)
install(TARGETS kvc EXPORT kvcTargets
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT kvcTargets NAMESPACE kvc:: DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/kvc)
