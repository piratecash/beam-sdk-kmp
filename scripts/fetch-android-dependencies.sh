#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
lock_file="$repo_root/native/android-dependencies.lock"
dependencies_dir="${1:-$repo_root/.native-cache/android-dependencies}"

value() {
    sed -n "s/^$1=//p" "$lock_file"
}

fetch_repository() {
    local name="$1"
    local repository="$2"
    local branch="$3"
    local commit="$4"
    local destination="$5"

    if [[ ! -d "$destination/.git" ]]; then
        mkdir -p "$(dirname "$destination")"
        git clone --filter=blob:none --no-checkout "$repository" "$destination"
        git -C "$destination" fetch --depth 1 origin "$commit"
        git -C "$destination" checkout --detach "$commit"
    fi
    local actual
    actual="$(git -C "$destination" rev-parse HEAD)"
    if [[ "$actual" != "$commit" ]]; then
        echo "$name mismatch: expected $commit, found $actual" >&2
        exit 1
    fi
}

fetch_repository \
    Boost \
    "$(value boost_repository)" \
    "$(value boost_branch)" \
    "$(value boost_commit)" \
    "$dependencies_dir/boost"
fetch_repository \
    OpenSSL \
    "$(value openssl_repository)" \
    "$(value openssl_branch)" \
    "$(value openssl_commit)" \
    "$dependencies_dir/openssl"

printf '%s\n' "$dependencies_dir"
