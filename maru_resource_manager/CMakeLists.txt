cmake_minimum_required(VERSION 3.16)
project(maru_resourced LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(GNUInstallDirs)

find_package(OpenSSL REQUIRED)

add_executable(maru_resourced
    src/util.cpp
    src/main.cpp
    src/log.cpp
    src/pool_manager.cpp
    src/metadata.cpp
    src/wal.cpp
    src/reaper.cpp
    src/uds_server.cpp
)

target_include_directories(maru_resourced PRIVATE include src
    ${CMAKE_CURRENT_SOURCE_DIR}/../marufs_kernel/include)

target_compile_options(maru_resourced PRIVATE -Wall -Wextra -Wpedantic)

target_link_libraries(maru_resourced PRIVATE pthread OpenSSL::Crypto)

install(TARGETS maru_resourced
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

# Install systemd service and udev rules
option(MARU_INSTALL_SYSTEMD "Install systemd service and udev rules" ON)

if(MARU_INSTALL_SYSTEMD)
    set(MARU_RESOURCED_BIN "${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_BINDIR}/maru_resourced")
    configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/systemd/maru-resourced.service
        ${CMAKE_CURRENT_BINARY_DIR}/maru-resourced.service
        @ONLY
    )
    install(FILES ${CMAKE_CURRENT_BINARY_DIR}/maru-resourced.service
        DESTINATION /etc/systemd/system
    )

    if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/systemd/99-maru-resourced.rules)
        install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/systemd/99-maru-resourced.rules
            DESTINATION /etc/udev/rules.d
        )
    endif()

    install(CODE "
        execute_process(COMMAND systemctl daemon-reload)
        if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/systemd/99-maru-resourced.rules)
            execute_process(COMMAND udevadm control --reload ERROR_QUIET)
            execute_process(COMMAND udevadm trigger --subsystem-match=dax ERROR_QUIET)
        endif()
        execute_process(COMMAND systemctl enable maru-resourced)
        execute_process(COMMAND systemctl restart maru-resourced)
        message(\"-- Systemd service installed and started\")
    ")
endif()
