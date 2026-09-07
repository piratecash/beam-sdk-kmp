#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
evidence="$repo_root/release-evidence.json"
expected_commit="${1:-}"
expected_core="$(sed -n 's/^commit=//p' "$repo_root/native/beam-core.lock")"

if [[ -z "$expected_commit" ]]; then
    echo "Usage: $0 <release-commit>" >&2
    exit 2
fi

tested_commit="$(jq -r '.testedCommit // empty' "$evidence")"
if [[ -z "$tested_commit" || "$tested_commit" == "PENDING" ]]; then
    echo "Release blocked: testedCommit is missing from release evidence" >&2
    exit 1
fi
if ! git -C "$repo_root" cat-file -e "$tested_commit^{commit}" 2>/dev/null; then
    echo "Release blocked: testedCommit is not present in repository history" >&2
    exit 1
fi
if ! git -C "$repo_root" merge-base --is-ancestor "$tested_commit" "$expected_commit"; then
    echo "Release blocked: testedCommit is not an ancestor of the release commit" >&2
    exit 1
fi
if ! git -C "$repo_root" diff --quiet "$tested_commit" "$expected_commit" -- . \
    ':(exclude)release-evidence.json'; then
    echo "Release blocked: source or build inputs changed after device testing" >&2
    exit 1
fi

jq -e \
    --arg commit "$tested_commit" \
    --arg core "$expected_core" \
    '
      .schemaVersion == 1 and
      .testedCommit == $commit and
      .coreCommit == $core and
      ([.android["arm64-v8a"], .android["armeabi-v7a"]] | all(
        .status == "passed" and
        (.device | type == "string" and length > 0 and . != "PENDING") and
        (.apiLevel | type == "number" and . >= 27) and
        (.nativeVersion | startswith("beam-7.5.14493+")) and
        .walletSmoke == "passed" and
        (.testedAtUtc | type == "string" and length > 0 and . != "PENDING")
      ))
    ' "$evidence" >/dev/null || {
        echo "Release blocked: arm64-v8a and armeabi-v7a device evidence must pass for $expected_commit" >&2
        exit 1
    }

echo "Release device evidence verified for $expected_commit"
