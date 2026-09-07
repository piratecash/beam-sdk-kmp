#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
core_dir="${BEAM_CORE_DIR:-$repo_root/.native-cache/beam-core}"
dependencies_dir="${BEAM_ANDROID_DEPENDENCIES_DIR:-$repo_root/.native-cache/android-dependencies}"
boost_dir="${BEAM_BOOST_ANDROID_DIR:-$dependencies_dir/boost}"
openssl_dir="${BEAM_OPENSSL_ANDROID_DIR:-$dependencies_dir/openssl}"
android_sdk="${ANDROID_HOME:-${ANDROID_SDK_ROOT:-}}"
ndk_version="$(sed -n 's/^beamSdk.ndkVersion=//p' "$repo_root/gradle.properties")"
ndk_dir="${ANDROID_NDK_HOME:-${android_sdk:+$android_sdk/ndk/$ndk_version}}"

if [[ ! -f "$core_dir/wallet/client/wallet_client.h" ]]; then
    echo "Beam Core not found at $core_dir; run scripts/fetch-beam-core.sh first" >&2
    exit 1
fi
if [[ -z "$ndk_dir" || ! -f "$ndk_dir/build/cmake/android.toolchain.cmake" ]]; then
    echo "Android NDK $ndk_version was not found; set ANDROID_NDK_HOME or ANDROID_HOME" >&2
    exit 1
fi
if [[ ! -d "$boost_dir/libs" || ! -d "$openssl_dir/libs" ]]; then
    echo "Pinned Android dependencies are missing; run scripts/fetch-android-dependencies.sh" >&2
    exit 1
fi
strip_tool="$(find "$ndk_dir/toolchains/llvm/prebuilt" -name llvm-strip -print -quit)"
if [[ -z "$strip_tool" ]]; then
    echo "llvm-strip was not found in Android NDK $ndk_version" >&2
    exit 1
fi

export BOOST_ROOT_ANDROID="$boost_dir"
export OPENSSL_ROOT_DIR_ANDROID="$openssl_dir"

abis="$(sed -n 's/^beamSdk.androidAbis=//p' "$repo_root/gradle.properties")"
IFS=',' read -r -a abi_list <<< "$abis"
for abi in "${abi_list[@]}"; do
    build_dir="${BEAM_ANDROID_BUILD_ROOT:-$repo_root/.native-cache/build-android}/$abi"
    cmake -S "$repo_root/native" -B "$build_dir" \
        -DBEAM_CORE_DIR="$core_dir" \
        -DCMAKE_TOOLCHAIN_FILE="$ndk_dir/build/cmake/android.toolchain.cmake" \
        -DANDROID_ABI="$abi" \
        -DANDROID_PLATFORM=android-27 \
        -DANDROID_STL=c++_static \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_CXX_FLAGS="-Wno-error=deprecated-literal-operator -Wno-deprecated-literal-operator"
    cmake --build "$build_dir" --target beam_sdk_kmp --parallel
    destination="$repo_root/beam-sdk/prebuilt/android/native/$abi"
    mkdir -p "$destination"
    binary="$(find "$build_dir" -type f -name libbeam_sdk_kmp.so -print -quit)"
    if [[ -z "$binary" ]]; then
        echo "Native Android binary was not produced for $abi" >&2
        exit 1
    fi
    "$strip_tool" --strip-unneeded "$binary"
    cp "$binary" "$destination/libbeam_sdk_kmp.so"
done
