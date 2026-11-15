#!/usr/bin/env bash
# 容器启动入口：先恢复产品管线运行状态（建表 + 示例回填 + 断点续跑），再起服务。
# 迁移中断过的部署会从最后一个完整节点继续；迁移幂等，重复执行不产生重复依赖边。
set -euo pipefail
cd "$(dirname "$0")"

export PIPELINE_AUTO_MIGRATE="${PIPELINE_AUTO_MIGRATE:-1}"
python -m app.persistence.cli init
python -m app.persistence.cli migrate || true

exec uvicorn app.main:app --host 0.0.0.0 --port 8000
