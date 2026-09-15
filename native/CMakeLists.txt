cmake_minimum_required(VERSION 3.24)

project(beam_sdk_kmp_native LANGUAGES C CXX)

if(NOT DEFINED BEAM_CORE_DIR)
    message(FATAL_ERROR "BEAM_CORE_DIR must point to Beam Core beam-7.5.14493")
endif()

get_filename_component(BEAM_CORE_DIR "${BEAM_CORE_DIR}" REALPATH)
if(NOT EXISTS "${BEAM_CORE_DIR}/wallet/client/wallet_client.h")
    message(FATAL_ERROR "BEAM_CORE_DIR does not contain a Beam Core checkout")
endif()

# Release binaries must not disclose or depend on the local checkout location. This also keeps
# diagnostics embedded through __FILE__ stable across CI hosts.
if(MSVC)
    add_compile_options("/experimental:deterministic")
    add_compile_options("/pathmap:${BEAM_CORE_DIR}=beam-core")
else()
    add_compile_options("-ffile-prefix-map=${BEAM_CORE_DIR}=beam-core")
    add_compile_options("-ffile-prefix-map=${CMAKE_CURRENT_SOURCE_DIR}=beam-sdk-native")
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_NO_SYSTEM_FROM_IMPORTED ON)
set(BEAM_WALLET_CLIENT_LIBRARY ON CACHE BOOL "" FORCE)
set(BEAM_TESTS_ENABLED OFF CACHE BOOL "" FORCE)
set(BEAM_ATOMIC_SWAP_SUPPORT OFF CACHE BOOL "" FORCE)
set(BEAM_ASSET_SWAP_SUPPORT OFF CACHE BOOL "" FORCE)
set(BEAM_IPFS_SUPPORT OFF CACHE BOOL "" FORCE)
set(BEAM_LASER_SUPPORT OFF CACHE BOOL "" FORCE)
set(BEAM_HW_WALLET OFF CACHE BOOL "" FORCE)
set(BEAM_BUILD_JNI OFF CACHE BOOL "" FORCE)
if(ANDROID)
    # Beam's Android dependency branch consumes the pinned prebuilt Boost/OpenSSL
    # archives below. BEAM_USE_STATIC selects the host find_package path instead.
    set(BEAM_USE_STATIC OFF CACHE BOOL "" FORCE)
else()
    set(BEAM_USE_STATIC ON CACHE BOOL "" FORCE)
endif()

# Xcode toolchains can inject /usr/local/include ahead of CMake's SYSTEM paths.
# Put the explicitly pinned package roots first so a stale Intel Homebrew Boost
# cannot be mixed with the arm64 libraries selected by find_package.
if(DEFINED ENV{BOOST_ROOT})
    include_directories(BEFORE "$ENV{BOOST_ROOT}/include")
endif()
if(DEFINED ENV{OPENSSL_ROOT_DIR})
    include_directories(BEFORE "$ENV{OPENSSL_ROOT_DIR}/include")
endif()

add_subdirectory("${BEAM_CORE_DIR}" "${CMAKE_BINARY_DIR}/beam-core" EXCLUDE_FROM_ALL)

if(NOT ANDROID)
    find_package(Java 21 REQUIRED COMPONENTS Development)
    find_package(JNI REQUIRED)
endif()

add_library(beam_sdk_offline_context STATIC src/offline_context.cpp)
target_include_directories(beam_sdk_offline_context PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/src"
    "${BEAM_CORE_DIR}" "${BEAM_CORE_DIR}/3rdparty")
target_link_libraries(beam_sdk_offline_context PUBLIC wallet_core)
add_library(beam_sdk_kmp SHARED src/beam_jni.cpp)
target_sources(beam_sdk_kmp PRIVATE src/stateless_codec.cpp src/stateless_codec_jni.cpp)
target_sources(beam_sdk_kmp PRIVATE src/offline_signer.cpp)
target_sources(beam_sdk_kmp PRIVATE src/transaction_relay.cpp src/transaction_relay_jni.cpp)
target_link_libraries(beam_sdk_kmp PRIVATE beam_sdk_offline_context)

target_include_directories(beam_sdk_kmp PRIVATE
    "${BEAM_CORE_DIR}"
    "${BEAM_CORE_DIR}/3rdparty"
)

if(NOT ANDROID)
    target_include_directories(beam_sdk_kmp PRIVATE ${JNI_INCLUDE_DIRS})
endif()

target_link_libraries(beam_sdk_kmp PRIVATE
    wallet_client_no_ipfs
    utility
)

if(ANDROID)
    target_link_libraries(beam_sdk_kmp PRIVATE android log)
    # Android 15 devices may use 16 KiB pages. A larger ELF max-page-size is
    # backwards compatible with 4 KiB devices and is mandatory for Play delivery.
    target_link_options(beam_sdk_kmp PRIVATE "-Wl,-z,max-page-size=16384")
endif()

target_compile_definitions(beam_sdk_kmp PRIVATE
    BEAM_SDK_CORE_COMMIT="9c4366aae08e7fbde7bc8d65f828a0deace68bfc"
)
if(MSVC)
    target_compile_definitions(beam_sdk_kmp PRIVATE NOMINMAX)
    target_compile_definitions(beam_sdk_offline_context PRIVATE NOMINMAX)
endif()

option(BEAM_SDK_KMP_BUILD_NATIVE_TESTS "Build native Beam SDK regression tests" OFF)
if(BEAM_SDK_KMP_BUILD_NATIVE_TESTS)
    target_compile_definitions(beam_sdk_kmp PRIVATE BEAM_SDK_KMP_TESTS)
    target_compile_definitions(wallet_core PRIVATE BEAM_SDK_KMP_TESTS)
    add_executable(beam_sdk_kmp_native_tests tests/wallet_db_durability_test.cpp)
    target_include_directories(beam_sdk_kmp_native_tests PRIVATE
        "${BEAM_CORE_DIR}"
        "${BEAM_CORE_DIR}/3rdparty"
    )
    target_compile_definitions(beam_sdk_kmp_native_tests PRIVATE BEAM_SDK_KMP_TESTS)
    if(MSVC)
        target_compile_definitions(beam_sdk_kmp_native_tests PRIVATE NOMINMAX)
    endif()
    target_link_libraries(beam_sdk_kmp_native_tests PRIVATE wallet_core)

    # Separate test binaries keep all admission controls out of the shipped JNI API.
    add_executable(beam_sdk_kmp_send_admission_tests tests/send_admission_test.cpp)
    add_library(beam_sdk_kmp_send_admission_fixture SHARED tests/send_admission_test.cpp)
    target_compile_definitions(beam_sdk_kmp_send_admission_tests PRIVATE BEAM_SDK_KMP_ADMISSION_TEST_MAIN)
    foreach(target beam_sdk_kmp_send_admission_tests beam_sdk_kmp_send_admission_fixture)
        target_include_directories(${target} PRIVATE "${BEAM_CORE_DIR}" "${BEAM_CORE_DIR}/3rdparty" ${JNI_INCLUDE_DIRS})
        target_compile_definitions(${target} PRIVATE BEAM_SDK_KMP_TESTS
            BEAM_SDK_CORE_COMMIT="9c4366aae08e7fbde7bc8d65f828a0deace68bfc")
        if(MSVC)
            target_compile_definitions(${target} PRIVATE NOMINMAX)
        endif()
        target_link_libraries(${target} PRIVATE wallet_client_no_ipfs utility beam_sdk_offline_context)
    endforeach()
    foreach(target beam_sdk_kmp_send_admission_tests beam_sdk_kmp_send_admission_fixture)
        target_sources(${target} PRIVATE src/offline_signer.cpp src/stateless_codec.cpp)
    endforeach()
    enable_testing()
    add_test(NAME send_admission COMMAND beam_sdk_kmp_send_admission_tests)
    add_executable(beam_sdk_kmp_snapshot_reorg_tests tests/snapshot_reorg_recovery_test.cpp)
    target_include_directories(beam_sdk_kmp_snapshot_reorg_tests PRIVATE "${BEAM_CORE_DIR}" "${BEAM_CORE_DIR}/3rdparty")
    target_compile_definitions(beam_sdk_kmp_snapshot_reorg_tests PRIVATE BEAM_SDK_KMP_TESTS)
    target_link_libraries(beam_sdk_kmp_snapshot_reorg_tests PRIVATE wallet_client_no_ipfs utility)
    add_test(NAME snapshot_reorg COMMAND beam_sdk_kmp_snapshot_reorg_tests)
    add_library(beam_sdk_kmp_snapshot_reorg_fixture SHARED tests/snapshot_reorg_recovery_test.cpp)
    target_include_directories(beam_sdk_kmp_snapshot_reorg_fixture PRIVATE
        "${BEAM_CORE_DIR}" "${BEAM_CORE_DIR}/3rdparty" ${JNI_INCLUDE_DIRS})
    target_compile_definitions(beam_sdk_kmp_snapshot_reorg_fixture PRIVATE BEAM_SDK_KMP_TESTS BEAM_SDK_KMP_REORG_JNI)
    target_link_libraries(beam_sdk_kmp_snapshot_reorg_fixture PRIVATE wallet_client_no_ipfs utility)
    if(MSVC)
        target_compile_definitions(beam_sdk_kmp_snapshot_reorg_tests PRIVATE NOMINMAX)
        target_compile_definitions(beam_sdk_kmp_snapshot_reorg_fixture PRIVATE NOMINMAX)
    endif()

    add_library(beam_sdk_kmp_offline_history_fixture SHARED tests/offline_history_test.cpp)
    target_include_directories(beam_sdk_kmp_offline_history_fixture PRIVATE
        "${BEAM_CORE_DIR}" "${BEAM_CORE_DIR}/3rdparty" ${JNI_INCLUDE_DIRS})
    target_compile_definitions(beam_sdk_kmp_offline_history_fixture PRIVATE BEAM_SDK_KMP_TESTS
        BEAM_SDK_CORE_COMMIT="9c4366aae08e7fbde7bc8d65f828a0deace68bfc")
    target_link_libraries(beam_sdk_kmp_offline_history_fixture PRIVATE wallet_client_no_ipfs utility beam_sdk_offline_context)
    if(MSVC)
        target_compile_definitions(beam_sdk_kmp_offline_history_fixture PRIVATE NOMINMAX)
    endif()
    target_sources(beam_sdk_kmp_offline_history_fixture PRIVATE src/offline_signer.cpp src/stateless_codec.cpp)
    if(NOT ANDROID)
        add_executable(beam_sdk_kmp_offline_signer_tests tests/offline_signer_test.cpp
            src/offline_signer.cpp src/stateless_codec.cpp)
        target_include_directories(beam_sdk_kmp_offline_signer_tests PRIVATE
            "${CMAKE_CURRENT_SOURCE_DIR}/src" "${BEAM_CORE_DIR}" "${BEAM_CORE_DIR}/3rdparty" ${JNI_INCLUDE_DIRS})
        target_compile_definitions(beam_sdk_kmp_offline_signer_tests PRIVATE BEAM_SDK_KMP_TESTS
            BEAM_SDK_CORE_COMMIT="9c4366aae08e7fbde7bc8d65f828a0deace68bfc")
        target_link_libraries(beam_sdk_kmp_offline_signer_tests PRIVATE wallet_client_no_ipfs utility beam_sdk_offline_context lelantus)
        add_test(NAME offline_signer COMMAND beam_sdk_kmp_offline_signer_tests)
        add_executable(beam_sdk_kmp_transaction_relay_tests tests/transaction_relay_test.cpp
            src/transaction_relay.cpp src/stateless_codec.cpp)
        target_include_directories(beam_sdk_kmp_transaction_relay_tests PRIVATE
            "${CMAKE_CURRENT_SOURCE_DIR}/src" "${BEAM_CORE_DIR}" "${BEAM_CORE_DIR}/3rdparty")
        target_compile_definitions(beam_sdk_kmp_transaction_relay_tests PRIVATE BEAM_SDK_KMP_TESTS)
        if(MSVC)
            target_compile_definitions(beam_sdk_kmp_transaction_relay_tests PRIVATE NOMINMAX)
            target_compile_definitions(beam_sdk_kmp_offline_signer_tests PRIVATE NOMINMAX)
        endif()
        target_link_libraries(beam_sdk_kmp_transaction_relay_tests PRIVATE lelantus)
        add_test(NAME transaction_relay COMMAND beam_sdk_kmp_transaction_relay_tests)
        add_executable(beam_sdk_kmp_stateless_codec_tests tests/stateless_codec_test.cpp src/stateless_codec.cpp)
        target_include_directories(beam_sdk_kmp_stateless_codec_tests PRIVATE
            "${CMAKE_CURRENT_SOURCE_DIR}/src" "${BEAM_CORE_DIR}" "${BEAM_CORE_DIR}/3rdparty")
        target_compile_definitions(beam_sdk_kmp_stateless_codec_tests PRIVATE BEAM_SDK_KMP_TESTS)
        if(MSVC)
            target_compile_definitions(beam_sdk_kmp_stateless_codec_tests PRIVATE NOMINMAX)
        endif()
        target_link_libraries(beam_sdk_kmp_stateless_codec_tests PRIVATE lelantus)
        add_test(NAME stateless_codec COMMAND beam_sdk_kmp_stateless_codec_tests)
        add_executable(beam_sdk_kmp_offline_signing_tests tests/offline_signing_test.cpp)
        target_include_directories(beam_sdk_kmp_offline_signing_tests PRIVATE
            "${BEAM_CORE_DIR}" "${BEAM_CORE_DIR}/3rdparty")
        target_compile_definitions(beam_sdk_kmp_offline_signing_tests PRIVATE BEAM_SDK_KMP_TESTS)
        if(MSVC)
            target_compile_definitions(beam_sdk_kmp_offline_signing_tests PRIVATE NOMINMAX)
        endif()
        target_link_libraries(beam_sdk_kmp_offline_signing_tests PRIVATE lelantus)
        add_test(NAME offline_signing COMMAND beam_sdk_kmp_offline_signing_tests)
        add_executable(beam_sdk_kmp_offline_context_tests tests/offline_context_test.cpp)
        target_link_libraries(beam_sdk_kmp_offline_context_tests PRIVATE beam_sdk_offline_context lelantus)
        target_compile_definitions(beam_sdk_kmp_offline_context_tests PRIVATE BEAM_SDK_KMP_TESTS)
        if(MSVC)
            target_compile_definitions(beam_sdk_kmp_offline_context_tests PRIVATE NOMINMAX)
        endif()
        add_test(NAME offline_context COMMAND beam_sdk_kmp_offline_context_tests)
    endif()
endif()

set_target_properties(beam_sdk_kmp PROPERTIES
    CXX_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN YES
)
