#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
if [ ! -d .venv ]; then
  python3 -m venv .venv
fi
.venv/bin/pip install -q -r requirements.txt

# 本地开发环境：建表回填既有示例节点，随后断点续跑产品管线迁移（幂等，可反复执行）。
export PIPELINE_AUTO_MIGRATE="${PIPELINE_AUTO_MIGRATE:-1}"
.venv/bin/python -m app.persistence.cli init
.venv/bin/python -m app.persistence.cli migrate || true

exec .venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8000
