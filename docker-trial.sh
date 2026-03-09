#!/usr/bin/env bash
set -euo pipefail
docker run --rm -v "$(cd "$(dirname "$1")" && pwd)":/data epl-trial "/data/$(basename "$1")"
