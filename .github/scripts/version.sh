#!/usr/bin/env bash
set -euo pipefail

python3 -c 'import tomllib; print(tomllib.load(open("pyproject.toml", "rb"))["project"]["version"])'
