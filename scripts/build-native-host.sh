#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
core_dir="${BEAM_CORE_DIR:-$repo_root/.native-cache/beam-core}"
build_dir="${BEAM_NATIVE_BUILD_DIR:-$repo_root/.native-cache/build-host}"
host_dependencies_environment="${BEAM_HOST_DEPENDENCIES_DIR:-$repo_root/.native-cache/host-dependencies}/environment.sh"

if [[ -f "$host_dependencies_environment" ]]; then
    # Generated only by fetch-host-dependencies.sh from immutable lock-file revisions.
    source "$host_dependencies_environment"
fi

if [[ ! -f "$core_dir/wallet/client/wallet_client.h" ]]; then
    echo "Beam Core not found at $core_dir; run scripts/fetch-beam-core.sh first" >&2
    exit 1
fi

case "$(uname -s)-$(uname -m)" in
    Darwin-arm64) triple="aarch64-apple-darwin" ;;
    Linux-x86_64) triple="x86_64-unknown-linux-gnu" ;;
    MINGW*-x86_64|MSYS*-x86_64) triple="x86_64-pc-windows-msvc" ;;
    *) echo "Unsupported host: $(uname -s)-$(uname -m)" >&2; exit 1 ;;
esac

cmake_args=(
    -S "$repo_root/native"
    -B "$build_dir"
    -DBEAM_CORE_DIR="$core_dir"
    -DCMAKE_BUILD_TYPE=Release
)
if [[ "${BEAM_NATIVE_TESTS:-0}" == "1" ]]; then
    cmake_args+=(-DBEAM_SDK_KMP_BUILD_NATIVE_TESTS=ON)
else
    cmake_args+=(-DBEAM_SDK_KMP_BUILD_NATIVE_TESTS=OFF)
fi
if [[ "$triple" == "aarch64-apple-darwin" ]]; then
    cmake_args+=(-DCMAKE_OSX_ARCHITECTURES=arm64)
fi
cmake "${cmake_args[@]}"

build_target() {
    local target="$1"
    if [[ "$triple" == "x86_64-pc-windows-msvc" ]]; then
        cmake --build "$build_dir" --config Release --target "$target" --parallel
    else
        cmake --build "$build_dir" --target "$target" --parallel
    fi
}

build_target beam_sdk_kmp
if [[ "${BEAM_NATIVE_TESTS:-0}" == "1" ]]; then
    build_target beam_sdk_kmp_native_tests
    if [[ "$triple" == "x86_64-pc-windows-msvc" ]]; then
        test_binary="$build_dir/Release/beam_sdk_kmp_native_tests.exe"
    else
        test_binary="$(find "$build_dir" -type f -name beam_sdk_kmp_native_tests -print -quit)"
    fi
    if [[ -z "$test_binary" || ! -f "$test_binary" ]]; then
        echo "Native regression-test binary was not produced" >&2
        exit 1
    fi
    "$test_binary"
fi

destination="$repo_root/beam-sdk/prebuilt/desktop/native/$triple"
mkdir -p "$destination"

case "$triple" in
    aarch64-apple-darwin) binary="libbeam_sdk_kmp.dylib" ;;
    x86_64-unknown-linux-gnu) binary="libbeam_sdk_kmp.so" ;;
    x86_64-pc-windows-msvc) binary="beam_sdk_kmp.dll" ;;
esac

if [[ "$triple" == "x86_64-pc-windows-msvc" ]]; then
    found="$build_dir/Release/$binary"
else
    found="$(find "$build_dir" -type f -name "$binary" -print -quit)"
fi
if [[ -z "$found" || ! -f "$found" ]]; then
    echo "Native binary was not produced: $binary" >&2
    exit 1
fi
cp "$found" "$destination/$binary"
printf '%s\n' "$destination/$binary"
