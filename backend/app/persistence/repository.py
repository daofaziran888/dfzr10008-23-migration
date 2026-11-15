"""产品管线运行状态 sqlite 仓储。

设计要点：

1. 节点、依赖边、迁移断点、回填日志、输出格式快照全部落 sqlite，进程重启后可完整恢复。
2. ``pipeline_nodes.node_code`` 与 ``pipeline_edges`` 的 (上游, 下游) 都带唯一约束：
   回填与新节点并发时，先完成落库的记录直接生效，后到的写入被忽略，不会产生重复依赖边。
3. 所有写操作在同一把进程内锁内串行，sqlite 侧再开 WAL 与 busy_timeout，兼容多线程部署。
4. 等待上游的节点有独立保护：前置节点未全部完成时，任何路径都不能把它改成运行中。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.persistence.config import MIGRATION_LOG_FILE_NAME, PersistenceSettings, settings

# 节点运行时状态（区别于旧 CRUD 里的流程状态：草稿/运行中/已暂停/已完成）
STATUS_WAITING = "等待上游"
STATUS_RUNNING = "运行中"
STATUS_COMPLETED = "已完成"
STATUS_FAILED = "失败"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS pipeline_nodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_code TEXT NOT NULL UNIQUE,
    node_name TEXT NOT NULL,
    upstream_code TEXT,
    runtime_status TEXT NOT NULL,
    retry_policy INTEGER NOT NULL DEFAULT 0,
    retry_used INTEGER NOT NULL DEFAULT 0,
    failure_code TEXT,
    output_format TEXT NOT NULL DEFAULT 'parquet',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_edges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    upstream_node TEXT NOT NULL,
    downstream_node TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (upstream_node, downstream_node)
);

CREATE TABLE IF NOT EXISTS pipeline_migration_checkpoints (
    scope TEXT PRIMARY KEY,
    last_completed_node TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_migration_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_code TEXT NOT NULL,
    phase TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_output_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_code TEXT NOT NULL UNIQUE,
    output_format TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class PipelineRepository:
    """管线运行状态仓储；同一进程内共享一把锁，保证回填与新增并发时落库顺序确定。"""

    def __init__(
        self,
        db_path: str | None = None,
        log_dir: str | None = None,
    ) -> None:
        self._db_path = Path(db_path or settings.db_path)
        self._log_dir = Path(log_dir or settings.migration_log_dir)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            str(self._db_path), check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self.init_schema()

    # ------------------------------------------------------------------ 基础
    @property
    def db_path(self) -> Path:
        return self._db_path

    @property
    def log_file(self) -> Path:
        return self._log_dir / MIGRATION_LOG_FILE_NAME

    def init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def reset(self) -> None:
        """清空管线运行状态表，仅供迁移器重置命令与测试隔离使用。"""
        with self._lock:
            for table in (
                "pipeline_edges",
                "pipeline_migration_logs",
                "pipeline_migration_checkpoints",
                "pipeline_output_snapshots",
                "pipeline_nodes",
            ):
                self._conn.execute(f"DELETE FROM {table}")

    # ------------------------------------------------------------------ 节点
    def upsert_node(self, node: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        """按节点编号落库；编号已存在时以先落库的记录为准，后到写入整体忽略。

        返回 ``(落库后的节点行, 本次是否新写入)``。
        """
        code = str(node["node_code"])
        now = _now()
        with self._lock:
            existing = self.get_node(code)
            if existing is not None:
                # 先完成落库的节点为准：不覆盖任何字段，直接返回既有行。
                return existing, False
            self._conn.execute(
                """
                INSERT INTO pipeline_nodes (
                    node_code, node_name, upstream_code, runtime_status,
                    retry_policy, retry_used, failure_code, output_format,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    code,
                    str(node.get("node_name") or code),
                    node.get("upstream_code"),
                    str(node.get("runtime_status") or STATUS_WAITING),
                    int(node.get("retry_policy", 0)),
                    int(node.get("retry_used", 0)),
                    node.get("failure_code"),
                    str(node.get("output_format") or "parquet"),
                    now,
                    now,
                ),
            )
            row = self.get_node(code)
            assert row is not None
            return row, True

    def get_node(self, node_code: str) -> dict[str, Any] | None:
        cur = self._conn.execute(
            "SELECT * FROM pipeline_nodes WHERE node_code = ?", (node_code,)
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def list_nodes(self) -> list[dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT * FROM pipeline_nodes ORDER BY node_code ASC"
        )
        return [dict(row) for row in cur.fetchall()]

    def update_status(
        self,
        node_code: str,
        runtime_status: str,
        *,
        failure_code: str | None = None,
        retry_used: int | None = None,
    ) -> dict[str, Any] | None:
        """显式状态流转（迁移回填不走这里，回填只通过 upsert_node 落历史状态）。"""
        with self._lock:
            node = self.get_node(node_code)
            if node is None:
                return None
            fields = ["runtime_status = ?", "updated_at = ?"]
            params: list[Any] = [runtime_status, _now()]
            if failure_code is not None:
                fields.append("failure_code = ?")
                params.append(failure_code)
            if retry_used is not None:
                fields.append("retry_used = ?")
                params.append(int(retry_used))
            params.append(node_code)
            self._conn.execute(
                f"UPDATE pipeline_nodes SET {', '.join(fields)} WHERE node_code = ?",
                params,
            )
            return self.get_node(node_code)

    def is_upstream_done(self, node_code: str) -> bool:
        """前置节点全部已完成才放行；没有前置节点视为可执行。"""
        node = self.get_node(node_code)
        if node is None or not node.get("upstream_code"):
            return True
        upstream = self.get_node(str(node["upstream_code"]))
        if upstream is None:
            return False
        return upstream["runtime_status"] == STATUS_COMPLETED

    def mark_running(self, node_code: str) -> tuple[dict[str, Any] | None, str]:
        """把节点置为运行中；等待上游的节点在前置未完成时被拒绝，等待态不会被提前改写。"""
        with self._lock:
            node = self.get_node(node_code)
            if node is None:
                return None, f"节点 {node_code} 不存在"
            if node["runtime_status"] == STATUS_WAITING and not self.is_upstream_done(node_code):
                return node, (
                    f"节点 {node_code} 仍在等待上游 {node['upstream_code']} 完成，"
                    "不能提前转为运行中"
                )
            return self.update_status(node_code, STATUS_RUNNING), "节点已置为运行中"

    # ------------------------------------------------------------------ 依赖边
    def upsert_edge(self, upstream_node: str, downstream_node: str) -> bool:
        """插入依赖边；(上游, 下游) 唯一约束保证重复回填不会产生第二条边。

        返回 ``True`` 表示新写入，``False`` 表示边已存在被去重。
        """
        with self._lock:
            cur = self._conn.execute(
                """
                INSERT OR IGNORE INTO pipeline_edges (upstream_node, downstream_node, created_at)
                VALUES (?, ?, ?)
                """,
                (upstream_node, downstream_node, _now()),
            )
            return cur.rowcount > 0

    def list_edges(self) -> list[dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT * FROM pipeline_edges ORDER BY id ASC"
        )
        return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------ 断点
    def get_checkpoint(self, scope: str = "default") -> dict[str, Any] | None:
        cur = self._conn.execute(
            "SELECT * FROM pipeline_migration_checkpoints WHERE scope = ?", (scope,)
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def save_checkpoint(self, last_completed_node: str, scope: str = "default") -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO pipeline_migration_checkpoints (scope, last_completed_node, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(scope) DO UPDATE SET
                    last_completed_node = excluded.last_completed_node,
                    updated_at = excluded.updated_at
                """,
                (scope, last_completed_node, _now()),
            )

    # ------------------------------------------------------------------ 日志
    def add_log(self, node_code: str, phase: str, detail: str = "") -> dict[str, Any]:
        """回填日志同时写库与本地日志文件，均按节点编号留痕。"""
        with self._lock:
            cur = self._conn.execute(
                """
                INSERT INTO pipeline_migration_logs (node_code, phase, detail, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (node_code, phase, detail, _now()),
            )
            entry = {
                "id": cur.lastrowid,
                "node_code": node_code,
                "phase": phase,
                "detail": detail,
                "created_at": _now(),
            }
            self._append_file_log(entry)
            return entry

    def list_logs(self, node_code: str | None = None) -> list[dict[str, Any]]:
        if node_code:
            cur = self._conn.execute(
                "SELECT * FROM pipeline_migration_logs WHERE node_code = ? ORDER BY id ASC",
                (node_code,),
            )
        else:
            cur = self._conn.execute(
                "SELECT * FROM pipeline_migration_logs ORDER BY id ASC"
            )
        return [dict(row) for row in cur.fetchall()]

    def _append_file_log(self, entry: dict[str, Any]) -> None:
        line = (
            f"{entry['created_at']} | 节点={entry['node_code']} | "
            f"阶段={entry['phase']} | {entry['detail']}\n"
        )
        with self.log_file.open("a", encoding="utf-8") as handle:
            handle.write(line)

    # ------------------------------------------------------------------ 输出格式快照
    def upsert_snapshot(
        self,
        source_code: str,
        output_format: str,
        detail: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """输出格式快照按来源编号唯一；同样遵循先落库者为准。"""
        payload = json.dumps(detail or {}, ensure_ascii=False, sort_keys=True)
        now = _now()
        with self._lock:
            existing = self.get_snapshot(source_code)
            if existing is not None:
                return existing, False
            self._conn.execute(
                """
                INSERT INTO pipeline_output_snapshots
                    (source_code, output_format, detail, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (source_code, output_format, payload, now, now),
            )
            row = self.get_snapshot(source_code)
            assert row is not None
            return row, True

    def get_snapshot(self, source_code: str) -> dict[str, Any] | None:
        cur = self._conn.execute(
            "SELECT * FROM pipeline_output_snapshots WHERE source_code = ?",
            (source_code,),
        )
        row = cur.fetchone()
        if not row:
            return None
        data = dict(row)
        try:
            data["detail"] = json.loads(data.get("detail") or "{}")
        except json.JSONDecodeError:
            data["detail"] = {}
        return data

    def list_snapshots(self) -> list[dict[str, Any]]:
        cur = self._conn.execute(
            "SELECT * FROM pipeline_output_snapshots ORDER BY source_code ASC"
        )
        rows: list[dict[str, Any]] = []
        for row in cur.fetchall():
            data = dict(row)
            try:
                data["detail"] = json.loads(data.get("detail") or "{}")
            except json.JSONDecodeError:
                data["detail"] = {}
            rows.append(data)
        return rows


_repo: PipelineRepository | None = None
_repo_lock = threading.Lock()


def configure(db_path: str | None = None, log_dir: str | None = None) -> PipelineRepository:
    """测试或多环境部署时用来替换默认仓储路径。"""
    global _repo
    with _repo_lock:
        if _repo is not None:
            _repo.close()
        _repo = PipelineRepository(db_path=db_path, log_dir=log_dir)
        return _repo


def get_repo() -> PipelineRepository:
    global _repo
    with _repo_lock:
        if _repo is None:
            _repo = PipelineRepository()
        return _repo
