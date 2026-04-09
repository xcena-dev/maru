cmake_minimum_required(VERSION 3.16)
project(maru_resource_manager LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(GNUInstallDirs)

find_package(OpenSSL REQUIRED)

add_executable(maru_resource_manager
    src/device_header.cpp
    src/util.cpp
    src/main.cpp
    src/server_config.cpp
    src/log.cpp
    src/pool_manager.cpp
    src/metadata.cpp
    src/wal.cpp
    src/reaper.cpp
    src/request_handler.cpp
    src/tcp_server.cpp
)

target_include_directories(maru_resource_manager PRIVATE include src)

target_compile_options(maru_resource_manager PRIVATE -Wall -Wextra -Wpedantic)

target_link_libraries(maru_resource_manager PRIVATE pthread OpenSSL::Crypto)

set_target_properties(maru_resource_manager PROPERTIES OUTPUT_NAME "maru-resource-manager")

install(TARGETS maru_resource_manager
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

# Test client tool (not installed by default)
option(MARU_BUILD_TOOLS "Build test/debug tools" ON)

if(MARU_BUILD_TOOLS)
    add_executable(maru_test_client
        tools/maru_test_client.cpp
    )
    target_include_directories(maru_test_client PRIVATE include)
    target_link_libraries(maru_test_client PRIVATE pthread)
endif()

# Unit tests (Google Test)
option(MARU_BUILD_TESTS "Build unit tests" ON)

if(MARU_BUILD_TESTS)
    find_package(GTest REQUIRED)
    enable_testing()

    # Sources needed by tests (everything except main.cpp)
    set(RM_LIB_SOURCES
        src/device_header.cpp
        src/util.cpp
        src/server_config.cpp
        src/log.cpp
        src/pool_manager.cpp
        src/metadata.cpp
        src/wal.cpp
        src/reaper.cpp
        src/request_handler.cpp
        src/tcp_server.cpp
    )

    add_executable(maru_rm_tests
        tests/test_device_header.cpp
        tests/test_pool_manager_uuid.cpp
        ${RM_LIB_SOURCES}
    )

    target_include_directories(maru_rm_tests PRIVATE include src)
    target_compile_options(maru_rm_tests PRIVATE -Wall -Wextra -Wpedantic)
    target_link_libraries(maru_rm_tests PRIVATE
        GTest::gtest_main
        pthread
        OpenSSL::Crypto
    )

    include(GoogleTest)
    gtest_discover_tests(maru_rm_tests)
endif()
