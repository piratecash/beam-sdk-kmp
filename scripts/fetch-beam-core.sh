#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
lock_file="$repo_root/native/beam-core.lock"
checkout_dir="${1:-$repo_root/.native-cache/beam-core}"

tag="$(sed -n 's/^tag=//p' "$lock_file")"
commit="$(sed -n 's/^commit=//p' "$lock_file")"
repository="$(sed -n 's/^repository=//p' "$lock_file")"

if [[ -z "$tag" || -z "$commit" || -z "$repository" ]]; then
    echo "Invalid Beam Core lock file: $lock_file" >&2
    exit 1
fi

if [[ ! -d "$checkout_dir/.git" ]]; then
    mkdir -p "$(dirname "$checkout_dir")"
    git clone --filter=blob:none --no-checkout "$repository" "$checkout_dir"
    git -C "$checkout_dir" fetch --depth 1 origin "$commit"
    git -C "$checkout_dir" checkout --detach "$commit"
fi

actual="$(git -C "$checkout_dir" rev-parse HEAD)"
if [[ "$actual" != "$commit" ]]; then
    echo "Beam Core mismatch: expected $commit, found $actual" >&2
    exit 1
fi

git -C "$checkout_dir" submodule update --init --recursive --depth 1

ensure_submodules_pristine() {
    if git -C "$checkout_dir" submodule status --recursive | grep -Eq '^[+-U]'; then
        echo "Beam Core submodules do not match the pinned revisions" >&2
        exit 1
    fi
    if ! git -C "$checkout_dir" submodule foreach --recursive --quiet \
        'git diff --quiet && git diff --cached --quiet && test -z "$(git ls-files --others --exclude-standard)"'; then
        echo "Beam Core submodules contain local modifications" >&2
        exit 1
    fi
}

ensure_submodules_pristine

patch_files=("$repo_root"/native/patches/*.patch)
patch_manifest="$commit"
for patch_file in "${patch_files[@]}"; do
    patch_manifest+=$'\n'"$(basename "$patch_file") $(git hash-object "$patch_file")"
done
patch_stack_digest="$(printf '%s' "$patch_manifest" | git hash-object --stdin)"
git_dir="$(git -C "$checkout_dir" rev-parse --absolute-git-dir)"
patch_state="$git_dir/beam-sdk-kmp-patch-stack"

if ! git -C "$checkout_dir" diff --cached --quiet HEAD; then
    echo "Beam Core checkout contains staged modifications" >&2
    exit 1
fi
if [[ -n "$(git -C "$checkout_dir" ls-files --others --exclude-standard)" ]]; then
    echo "Beam Core checkout contains untracked files" >&2
    exit 1
fi

if [[ -f "$patch_state" ]]; then
    read -r recorded_stack_digest recorded_tree_digest < "$patch_state"
    if [[ "$recorded_stack_digest" != "$patch_stack_digest" ]]; then
        echo "Beam Core checkout was prepared with a different patch stack; use a clean checkout" >&2
        exit 1
    fi
    current_tree_digest="$(git -C "$checkout_dir" diff --binary HEAD | git hash-object --stdin)"
    if [[ "$recorded_tree_digest" != "$current_tree_digest" ]]; then
        echo "Beam Core checkout changed after applying the recorded patch stack" >&2
        exit 1
    fi
    git -C "$checkout_dir" diff --check HEAD
    printf '%s\n' "$checkout_dir"
    exit 0
fi

if ! git -C "$checkout_dir" diff --quiet HEAD; then
    echo "Beam Core checkout must be pristine before applying the SDK patch stack" >&2
    exit 1
fi

for patch_file in "${patch_files[@]}"; do
    git -C "$checkout_dir" apply --check "$patch_file"
    git -C "$checkout_dir" apply "$patch_file"
done

git -C "$checkout_dir" diff --check HEAD
patched_tree_digest="$(git -C "$checkout_dir" diff --binary HEAD | git hash-object --stdin)"
patch_state_temp="$patch_state.tmp.$$"
printf '%s %s\n' "$patch_stack_digest" "$patched_tree_digest" > "$patch_state_temp"
mv "$patch_state_temp" "$patch_state"
printf '%s\n' "$checkout_dir"
