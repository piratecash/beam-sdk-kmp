#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
lock_file="$repo_root/native/host-dependencies.lock"
dependencies_root="${BEAM_HOST_DEPENDENCIES_DIR:-$repo_root/.native-cache/host-dependencies}"

read_lock() {
    local key="$1"
    local value
    value="$(sed -n "s/^${key}=//p" "$lock_file")"
    if [[ -z "$value" ]]; then
        echo "Missing $key in $lock_file" >&2
        exit 1
    fi
    printf '%s' "$value"
}

clone_exact() {
    local repository="$1"
    local commit="$2"
    local destination="$3"
    if [[ ! -d "$destination/.git" ]]; then
        if [[ -e "$destination" ]]; then
            echo "Dependency path exists but is not a Git checkout: $destination" >&2
            exit 1
        fi
        git clone --filter=blob:none --no-checkout "$repository" "$destination"
        git -C "$destination" fetch --depth 1 origin "$commit"
        git -C "$destination" checkout --detach "$commit"
    fi
    local actual
    actual="$(git -C "$destination" rev-parse HEAD)"
    if [[ "$actual" != "$commit" ]]; then
        echo "Dependency checkout mismatch at $destination: expected $commit, found $actual" >&2
        exit 1
    fi
    if [[ -n "$(git -C "$destination" status --porcelain --untracked-files=no)" ]]; then
        echo "Dependency checkout has tracked modifications: $destination" >&2
        exit 1
    fi
}

mkdir -p "$dependencies_root"
environment_file="$dependencies_root/environment.sh"

case "$(uname -s)-$(uname -m)" in
    Darwin-arm64)
        boost_repository="$(read_lock boost_macos_repository)"
        boost_commit="$(read_lock boost_macos_commit)"
        openssl_target="darwin64-arm64-cc"
        ;;
    Linux-x86_64)
        boost_repository="$(read_lock boost_linux_repository)"
        boost_commit="$(read_lock boost_linux_commit)"
        openssl_target="linux-x86_64"
        ;;
    MINGW*-x86_64|MSYS*-x86_64)
        boost_repository="$(read_lock boost_windows_repository)"
        boost_commit="$(read_lock boost_windows_commit)"
        boost_directory="$dependencies_root/boost"
        windows_libraries_directory="$dependencies_root/windows-libraries"
        clone_exact "$boost_repository" "$boost_commit" "$boost_directory"
        clone_exact \
            "$(read_lock windows_libraries_repository)" \
            "$(read_lock windows_libraries_commit)" \
            "$windows_libraries_directory"
        printf 'export BOOST_ROOT=%q\nexport OPENSSL_ROOT_DIR=%q\n' \
            "$boost_directory" "$windows_libraries_directory/openssl" > "$environment_file"
        printf '%s\n' "$environment_file"
        exit 0
        ;;
    *)
        echo "Unsupported host for pinned native dependencies: $(uname -s)-$(uname -m)" >&2
        exit 1
        ;;
esac

boost_directory="$dependencies_root/boost"
clone_exact "$boost_repository" "$boost_commit" "$boost_directory"

openssl_repository="$(read_lock openssl_repository)"
openssl_commit="$(read_lock openssl_commit)"
openssl_source="$dependencies_root/openssl-source"
openssl_build="$dependencies_root/openssl-build-$openssl_target-static-v2"
openssl_stage="$dependencies_root/openssl-$openssl_commit-$openssl_target-static-v2"
openssl_install="$openssl_stage/beam-sdk/openssl"
clone_exact "$openssl_repository" "$openssl_commit" "$openssl_source"

if [[ ! -f "$openssl_install/lib/libcrypto.a" || ! -f "$openssl_install/lib/libssl.a" ]]; then
    mkdir -p "$openssl_build" "$openssl_stage"
    build_jobs="$(getconf _NPROCESSORS_ONLN 2>/dev/null || true)"
    if [[ -z "$build_jobs" ]]; then
        build_jobs="$(sysctl -n hw.ncpu 2>/dev/null || printf '2')"
    fi
    (
        cd "$openssl_build"
        "$openssl_source/Configure" "$openssl_target" no-shared no-tests no-dso no-module \
            --libdir=lib --prefix=/beam-sdk/openssl --openssldir=/beam-sdk/openssl/ssl
        make -j"$build_jobs"
        make DESTDIR="$openssl_stage" install_sw
    )
fi

printf 'export BOOST_ROOT=%q\nexport OPENSSL_ROOT_DIR=%q\n' \
    "$boost_directory" "$openssl_install" > "$environment_file"
printf '%s\n' "$environment_file"
