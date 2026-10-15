cmake_minimum_required(VERSION 3.16)
project(maru_resource_manager LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(GNUInstallDirs)

find_package(OpenSSL REQUIRED)

add_executable(maru_resource_manager
    src/admin_server.cpp
    src/device_exclusion.cpp
    src/device_header.cpp
    src/device_wipe.cpp
    src/util.cpp
    src/main.cpp
    src/server_config.cpp
    src/log.cpp
    src/pool_manager.cpp
    src/pool_validation.cpp
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
install(PROGRAMS tools/maru_rm_ctl.py
    DESTINATION ${CMAKE_INSTALL_BINDIR}
    RENAME maru-rm-ctl
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
        src/admin_server.cpp
        src/device_exclusion.cpp
        src/device_header.cpp
        src/device_wipe.cpp
        src/util.cpp
        src/server_config.cpp
        src/log.cpp
        src/pool_manager.cpp
        src/pool_validation.cpp
        src/metadata.cpp
        src/wal.cpp
        src/reaper.cpp
        src/request_handler.cpp
        src/tcp_server.cpp
    )

    add_executable(maru_rm_tests
        tests/test_device_header.cpp
        tests/test_pool_manager_uuid.cpp
        tests/test_dax_region_index.cpp
        tests/test_wal_replay.cpp
        tests/test_device_retry.cpp
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

    # The private /dev and /sys (tests/fake_dax_fs.cpp) and the checkpoint
    # failpoints (tests/checkpoint_fault.cpp) replace these calls.
    set(TEST_SYSCALL_WRAPS
        -Wl,--wrap=stat -Wl,--wrap=open -Wl,--wrap=opendir -Wl,--wrap=readlink
        -Wl,--wrap=write -Wl,--wrap=fsync -Wl,--wrap=fdatasync -Wl,--wrap=ftruncate)

    add_executable(maru_rm_recovery_tests
        tests/test_header_recovery.cpp tests/test_device_exclusion.cpp
        tests/fake_dax_fs.cpp tests/checkpoint_fault.cpp ${RM_LIB_SOURCES})
    target_include_directories(maru_rm_recovery_tests PRIVATE include src)
    target_compile_options(maru_rm_recovery_tests PRIVATE -Wall -Wextra -Wpedantic)
    target_link_options(maru_rm_recovery_tests PRIVATE ${TEST_SYSCALL_WRAPS})
    target_link_libraries(maru_rm_recovery_tests PRIVATE GTest::gtest_main pthread OpenSSL::Crypto)
    gtest_discover_tests(maru_rm_recovery_tests)

    # Real main, signals, TCP and persistence, with device paths redirected to
    # temporary files. This fixture executable is not an install target.
    find_package(Python3 3.9 REQUIRED COMPONENTS Interpreter)
    add_executable(maru_rm_process_test_daemon
        src/main.cpp tests/process_test_device.cpp tests/fake_dax_fs.cpp
        tests/checkpoint_fault.cpp ${RM_LIB_SOURCES})
    target_include_directories(maru_rm_process_test_daemon PRIVATE include src)
    target_compile_options(maru_rm_process_test_daemon PRIVATE -Wall -Wextra -Wpedantic)
    target_link_options(maru_rm_process_test_daemon PRIVATE
        -Wl,--wrap=main -Wl,--wrap=bind ${TEST_SYSCALL_WRAPS})
    target_link_libraries(maru_rm_process_test_daemon PRIVATE pthread OpenSSL::Crypto)
    foreach(scenario IN ITEMS
        sigterm_preserves_live_handle
        sigkill_replays_checkpoint_and_wal
        missing_secret_rejects_live_recovery
        prepared_legacy_checkpoint_round_trip
        shutdown_checkpoint_failure_retains_wal
        admin_exclude_and_include
        second_instance_is_refused)
        add_test(NAME ProcessRecovery.${scenario}
            COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/process_recovery_test.py
                $<TARGET_FILE:maru_rm_process_test_daemon> ProcessRecoveryTest.test_${scenario} -v)
        set_tests_properties(ProcessRecovery.${scenario} PROPERTIES TIMEOUT 60)
    endforeach()
endif()
