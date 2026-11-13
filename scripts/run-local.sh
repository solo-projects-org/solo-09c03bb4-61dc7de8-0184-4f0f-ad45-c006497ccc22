#!/usr/bin/env bash
# 本地启动（无 Docker 环境时使用）：优先使用项目内 .venv，其次系统 python。
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -x ".venv/bin/python" ]; then
  PY=".venv/bin/python"
else
  PY="python3"
fi
export REPRO_DATA_DIR="${REPRO_DATA_DIR:-$(pwd)/data}"
mkdir -p "$REPRO_DATA_DIR"
exec "$PY" -m uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8000}"
