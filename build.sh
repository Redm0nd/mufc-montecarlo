#!/usr/bin/env bash
# Package the handler with numpy built for Lambda arm64 / Python 3.14
set -euo pipefail
cd "$(dirname "$0")"
rm -rf build dist && mkdir -p build dist
pip install numpy \
  --platform manylinux_2_28_aarch64 \
  --python-version 3.14 \
  --only-binary=:all: \
  --target build \
  --quiet
cp src/handler.py build/
echo "build/ ready: $(du -sh build | cut -f1)"
