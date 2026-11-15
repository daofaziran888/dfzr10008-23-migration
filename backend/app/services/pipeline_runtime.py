"""产品管线运行时服务：读取持久化的运行结果、依赖边、断点与输出格式快照。

旧的 :class:`~app.services.pipeline.PipelineService` 继续负责内存示例的 CRUD，
本服务只读/驱动持久层，二者按节点编号互通，互不覆盖。
"""
from __future__ import annotations

from typing import Any

from app.persistence.migrator import PipelineMigrator
from app.persistence.repository import get_repo


class PipelineRuntimeService:
    def __init__(self) -> None:
        self.repo = get_repo()
        self.migrator = PipelineMigrator(self.repo)

    def list_runtime_nodes(self) -> list[dict[str, Any]]:
        """按节点编号返回迁移恢复后的运行结果。"""
        return self.repo.list_nodes()

    def get_runtime_node(self, node_code: str) -> dict[str, Any] | None:
        return self.repo.get_node(node_code)

    def list_edges(self) -> list[dict[str, Any]]:
        return self.repo.list_edges()

    def list_snapshots(self) -> list[dict[str, Any]]:
        return self.repo.list_snapshots()

    def get_snapshot(self, node_code: str) -> dict[str, Any] | None:
        return self.repo.get_snapshot(node_code)

    def migration_status(self) -> dict[str, Any]:
        """迁移断点与回填日志，日志按节点编号记录开始、完成和中断位置。"""
        return {
            "checkpoint": self.repo.get_checkpoint(),
            "logs": self.repo.list_logs(),
            "log_file": str(self.repo.log_file),
        }

    def resume_migration(
        self,
        *,
        interrupt_after: str | None = None,
        extra_nodes: list[dict[str, Any]] | None = None,
        extra_edges: list[tuple[str, str]] | None = None,
    ) -> dict[str, Any]:
        """触发一轮（断点）续跑；等待上游的节点状态由迁移样例固定，不会被改成运行中。"""
        return self.migrator.run(
            interrupt_after=interrupt_after,
            extra_nodes=extra_nodes,
            extra_edges=extra_edges,
        )

    def mark_running(self, node_code: str) -> tuple[dict[str, Any] | None, str]:
        """运行态转换入口：前置节点未完成时，等待中的节点不得提前变成运行中。"""
        return self.repo.mark_running(node_code)


runtime_service = PipelineRuntimeService()
