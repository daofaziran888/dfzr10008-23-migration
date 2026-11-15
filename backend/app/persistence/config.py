"""管线持久化运行配置。

路径全部允许通过环境变量覆盖，便于区分本地开发、构建产物与上线部署环境：

- ``PIPELINE_DB_PATH``：sqlite 数据文件位置，重启后节点、依赖边与断点都从这里恢复。
- ``PIPELINE_MIGRATION_LOG_DIR``：回填日志目录，按节点编号记录开始、完成与中断位置。
- ``PIPELINE_AUTO_MIGRATE``：置为 ``1`` 时，服务启动前自动执行断点续跑迁移。
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

_BASE_DIR = Path(__file__).resolve().parent.parent
_DEFAULT_DATA_DIR = _BASE_DIR / "data"

MIGRATION_LOG_FILE_NAME = "pipeline-migration.log"


@dataclass(frozen=True)
class PersistenceSettings:
    db_path: str = field(
        default_factory=lambda: os.getenv(
            "PIPELINE_DB_PATH",
            str(_DEFAULT_DATA_DIR / "pipeline_state.sqlite3"),
        )
    )
    migration_log_dir: str = field(
        default_factory=lambda: os.getenv(
            "PIPELINE_MIGRATION_LOG_DIR",
            str(_DEFAULT_DATA_DIR / "logs"),
        )
    )
    auto_migrate: bool = field(
        default_factory=lambda: os.getenv("PIPELINE_AUTO_MIGRATE", "0") == "1"
    )


settings = PersistenceSettings()
