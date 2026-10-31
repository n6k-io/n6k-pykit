#!/usr/bin/env bash
set -euo pipefail

name=$(python3 -c 'import tomllib; print(tomllib.load(open("pyproject.toml", "rb"))["project"]["name"])')
version=$("$(dirname "$0")/version.sh")

status=$(curl -s -o /dev/null -w '%{http_code}' "https://pypi.org/pypi/${name}/${version}/json")
case "$status" in
  404) echo "${name}==${version} not on PyPI" ;;
  200) echo "::error file=pyproject.toml::${name}==${version} is already on PyPI - bump the version"; exit 1 ;;
  *) echo "::error::unexpected PyPI response ${status}"; exit 1 ;;
esac
