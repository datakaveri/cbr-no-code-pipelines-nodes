#!/usr/bin/env sh
# Refresh the committed sdk/ copy from the upstream SDK repo.
#
# The node-builder service does a plain clone of this repo (no submodules, no
# credentials for the private sdk repo), so the Docker build can only use files
# committed here. sdk-upstream/ is the submodule pinning the exact upstream
# commit; sdk/ is the plain copy of its py-sdk/ that the Dockerfile installs.
# This script advances the submodule to the branch .gitmodules tracks and
# re-syncs sdk/ — review the diff and commit both together.
set -eu
cd "$(dirname "$0")"

# Single source of truth for the tracked branch, so this script and the
# submodule config cannot drift apart.
branch=$(git config -f "$(git rev-parse --show-toplevel)/.gitmodules" \
    submodule.omop-wide-table-source/sdk.branch)

git submodule update --init sdk-upstream
git -C sdk-upstream fetch origin
git -C sdk-upstream checkout --detach "origin/$branch"

rsync -a --delete \
    --exclude '__pycache__' --exclude '*.egg-info' \
    --exclude 'build' --exclude 'dist' \
    sdk-upstream/py-sdk/ sdk/

echo "sdk/ synced to upstream $branch $(git -C sdk-upstream rev-parse --short HEAD)."
echo "Review with 'git diff', then commit sdk/ and sdk-upstream together."
