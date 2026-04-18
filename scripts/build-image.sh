#!/usr/bin/env bash
set -euo pipefail

IMAGE_TAG="${IMAGE_TAG:-chdb-aws:local}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

docker build \
  --platform linux/amd64 \
  -f "${REPO_ROOT}/docker/Dockerfile" \
  -t "${IMAGE_TAG}" \
  "${REPO_ROOT}"
