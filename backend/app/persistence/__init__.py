"""产品管线运行状态持久化与迁移。

- ``repository``：sqlite3 仓储，节点、依赖边、迁移断点、回填日志与输出格式快照都落库，重启不丢。
- ``migrator``：按节点编号顺序回填迁移样例，支持断点续跑、并发去重与等待态保护。
- ``cli``：初始化、迁移、状态核对、重置的命令行入口，供本地、构建与部署环境调用。
"""
from __future__ import annotations

from app.persistence.config import MIGRATION_LOG_FILE_NAME, settings
from app.persistence.migrator import (
    DEPENDENCY_EDGES,
    PIPELINE_SAMPLES,
    MigrationInterrupted,
    PipelineMigrator,
)
from app.persistence.repository import PipelineRepository, configure, get_repo

__all__ = [
    "MIGRATION_LOG_FILE_NAME",
    "DEPENDENCY_EDGES",
    "PIPELINE_SAMPLES",
    "MigrationInterrupted",
    "PipelineMigrator",
    "PipelineRepository",
    "configure",
    "get_repo",
    "settings",
]
