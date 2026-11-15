"""数据产品与API工厂 后端服务入口。

启动：uvicorn app.main:app --host 127.0.0.1 --port 8000
健康检查：GET /api/health

启动阶段会恢复产品管线运行状态：建表、幂等回填既有示例节点，并在
PIPELINE_AUTO_MIGRATE=1 时自动执行断点续跑迁移；中断重启后从最后一个完整节点继续。
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.persistence.config import settings as persistence_settings
from app.persistence.migrator import MigrationInterrupted, PipelineMigrator
from app.persistence.repository import get_repo
from app.routers import ROUTERS
from app.seed import SEED_ROWS
from app.store import store

logger = logging.getLogger("app.startup")


@asynccontextmanager
async def lifespan(app: FastAPI) -> object:
    """恢复管线运行状态：仓储初始化、种子回填与断点续跑都在启动阶段完成。"""
    repo = get_repo()
    migrator = PipelineMigrator(repo)
    seed_result = migrator.backfill_seed_nodes(SEED_ROWS.get("pipeline", []))
    if seed_result["backfilled"]:
        logger.info("管线示例节点回填：%s", "、".join(seed_result["backfilled"]))
    if persistence_settings.auto_migrate:
        try:
            result = migrator.run()
            logger.info("管线迁移续跑完成：%s", result)
        except MigrationInterrupted as exc:  # pragma: no cover - 演练中断路径
            logger.warning("管线迁移仍处中断状态：%s", exc)
    yield


app = FastAPI(title="数据产品与API工厂", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

for module in ROUTERS:
    app.include_router(module.router)


@app.get("/api/health")
def health() -> dict[str, object]:
    """健康检查：确认服务已经监听、示例数据已经就绪、持久层可访问。"""
    repo = get_repo()
    return {
        "ok": True,
        "app": settings.app_name,
        "modules": len(store.module_names()),
        "pipeline_nodes": len(repo.list_nodes()),
        "pipeline_edges": len(repo.list_edges()),
    }


@app.get("/api/overview")
def overview() -> dict[str, object]:
    """运营概览：把各业务模块的待处理量汇总成看板卡片。"""
    return store.overview()
