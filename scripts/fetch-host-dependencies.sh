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

build_linux_pic_boost_filesystem() {
    local boost_directory="$1"
    local filesystem_repository
    local filesystem_commit
    local filesystem_source
    local boost_pic_root
    local boost_cxx
    local boost_ar
    local source_name
    local source_file
    local object_file
    local library
    local library_name
    local archive_candidate
    local -a filesystem_sources
    local -a filesystem_objects
    local -a compile_flags

    filesystem_repository="$(read_lock boost_filesystem_repository)"
    filesystem_commit="$(read_lock boost_filesystem_commit)"
    filesystem_source="$dependencies_root/boost-filesystem-source"
    boost_pic_root="$dependencies_root/boost-linux-pic-$filesystem_commit"
    boost_cxx="${CXX:-c++}"
    boost_ar="${AR:-ar}"

    clone_exact "$filesystem_repository" "$filesystem_commit" "$filesystem_source"
    mkdir -p "$boost_pic_root/lib/cmake" "$boost_pic_root/objects"
    ln -sfn "$boost_directory/include" "$boost_pic_root/include"
    cp -R "$boost_directory/lib/cmake/." "$boost_pic_root/lib/cmake/"

    for library in "$boost_directory/lib/"*; do
        library_name="${library##*/}"
        if [[ "$library_name" != "cmake" && "$library_name" != "libboost_filesystem.a" ]]; then
            ln -sfn "$library" "$boost_pic_root/lib/$library_name"
        fi
    done

    filesystem_sources=(
        codecvt_error_category.cpp
        exception.cpp
        directory.cpp
        operations.cpp
        path.cpp
        path_traits.cpp
        portability.cpp
        unique_path.cpp
        utf8_codecvt_facet.cpp
    )
    compile_flags=(
        -std=c++17
        -O2
        -fPIC
        -DBOOST_ALL_NO_LIB
        -DBOOST_FILESYSTEM_NO_LIB
        -DBOOST_FILESYSTEM_SOURCE
        -DBOOST_FILESYSTEM_STATIC_LINK=1
        -DBOOST_FILESYSTEM_NO_CXX20_ATOMIC_REF
        "-I$boost_directory/include"
        "-I$filesystem_source/src"
        "-ffile-prefix-map=$filesystem_source=boost-filesystem"
        "-ffile-prefix-map=$boost_directory=boost"
    )
    filesystem_objects=()
    for source_name in "${filesystem_sources[@]}"; do
        source_file="$filesystem_source/src/$source_name"
        object_file="$boost_pic_root/objects/${source_name%.cpp}.o"
        "$boost_cxx" "${compile_flags[@]}" -c "$source_file" -o "$object_file"
        filesystem_objects+=("$object_file")
    done

    archive_candidate="$boost_pic_root/lib/libboost_filesystem.a.$$.tmp"
    "$boost_ar" rcsD "$archive_candidate" "${filesystem_objects[@]}"
    mv "$archive_candidate" "$boost_pic_root/lib/libboost_filesystem.a"
    linux_boost_pic_directory="$boost_pic_root"
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
        rebuild_boost_filesystem=true
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
if [[ "${rebuild_boost_filesystem:-false}" == "true" ]]; then
    build_linux_pic_boost_filesystem "$boost_directory"
    boost_directory="$linux_boost_pic_directory"
fi

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
