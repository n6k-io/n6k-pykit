#!/usr/bin/env bash
set -euo pipefail

tag="v$("$(dirname "$0")/version.sh")"

if git ls-remote --exit-code --tags origin "refs/tags/${tag}" >/dev/null; then
  echo "${tag} already exists"
  exit 0
fi

git tag "$tag"
git push origin "$tag"
