#!/usr/bin/env bash
set -euo pipefail
docker run --rm -v "$(cd "$(dirname "$1")" && pwd)":/data helix-trial-community "/data/$(basename "$1")"
