"""管线运行状态迁移命令行入口。

用法（在 backend 目录下）::

    python -m app.persistence.cli init       # 建库建表 + 回填既有示例节点
    python -m app.persistence.cli migrate    # 执行/续跑产品管线迁移
    python -m app.persistence.cli status     # 查看节点、依赖边、断点与日志
    python -m app.persistence.cli verify     # 核对验收口径（失败码、状态、快照、去重）
    python -m app.persistence.cli reset      # 清空持久层（重新演练迁移用）

覆盖四类环境：本地开发直接执行；构建产物在镜像 entrypoint 中执行 init+migrate；
上线部署由容器启动脚本续跑；路径通过 PIPELINE_DB_PATH / PIPELINE_MIGRATION_LOG_DIR 覆盖。
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from app.persistence.migrator import (
    DEPENDENCY_EDGES,
    PIPELINE_SAMPLES,
    MigrationInterrupted,
    PipelineMigrator,
)
from app.persistence.repository import (
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_WAITING,
    PipelineRepository,
)
from app.seed import SEED_ROWS


def _build_repo(args: argparse.Namespace) -> PipelineRepository:
    return PipelineRepository(db_path=args.db_path, log_dir=args.log_dir)


def _print(payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def cmd_init(args: argparse.Namespace) -> int:
    repo = _build_repo(args)
    migrator = PipelineMigrator(repo)
    seed_result = migrator.backfill_seed_nodes(SEED_ROWS.get("pipeline", []))
    _print(
        {
            "ok": True,
            "db_path": str(repo.db_path),
            "log_file": str(repo.log_file),
            "seed_backfill": seed_result,
            "node_count": len(repo.list_nodes()),
        }
    )
    return 0


def cmd_migrate(args: argparse.Namespace) -> int:
    repo = _build_repo(args)
    migrator = PipelineMigrator(repo)
    try:
        result = migrator.run(interrupt_after=args.interrupt_after)
    except MigrationInterrupted as exc:
        _print(
            {
                "ok": False,
                "interrupted": True,
                "message": str(exc),
                "checkpoint": (repo.get_checkpoint() or {}).get("last_completed_node"),
            }
        )
        return 2
    _print({"ok": True, **result})
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    repo = _build_repo(args)
    _print(
        {
            "db_path": str(repo.db_path),
            "checkpoint": repo.get_checkpoint(),
            "nodes": repo.list_nodes(),
            "edges": repo.list_edges(),
            "snapshots": repo.list_snapshots(),
            "logs": repo.list_logs(),
        }
    )
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    """按验收口径逐项核对，任何一项不符就以非零码退出，方便流水线卡点。"""
    repo = _build_repo(args)
    problems: list[str] = []

    expected = {str(item["node_code"]): item for item in PIPELINE_SAMPLES}
    for code, sample in expected.items():
        node = repo.get_node(code)
        if node is None:
            problems.append(f"{code} 未落库")
            continue
        if node["runtime_status"] != sample["runtime_status"]:
            problems.append(
                f"{code} 运行状态应为 {sample['runtime_status']}，实际为 {node['runtime_status']}"
            )
        if int(node["retry_policy"]) != int(sample["retry_policy"]):
            problems.append(
                f"{code} 重试策略应为 {sample['retry_policy']}，实际为 {node['retry_policy']}"
            )
        if node["output_format"] != "parquet":
            problems.append(f"{code} 输出格式快照应为 parquet，实际为 {node['output_format']}")

    pl2 = repo.get_node("PL-0002")
    if pl2 is None or pl2.get("failure_code") != "ORA-LOAD-17":
        problems.append("PL-0002 失败码应为 ORA-LOAD-17")

    edges = {(row["upstream_node"], row["downstream_node"]) for row in repo.list_edges()}
    for edge in DEPENDENCY_EDGES:
        if edge not in edges:
            problems.append(f"依赖边 {edge[0]}->{edge[1]} 缺失")
    if len(edges) != len(repo.list_edges()):
        problems.append("依赖边出现重复")
    if len(repo.list_edges()) != len(edges):
        problems.append("依赖边表存在重复行")

    pl1_snapshot = repo.get_snapshot("PL-0001")
    if pl1_snapshot is None or pl1_snapshot.get("output_format") != "parquet":
        problems.append("PL-0001 输出格式快照无法读取或不是 parquet")

    pl3 = repo.get_node("PL-0003")
    if pl3 is not None and pl3["runtime_status"] != STATUS_WAITING:
        problems.append("PL-0003 等待态被改写，迁移器不得把等待上游改成运行中")

    phases = {(log["node_code"], log["phase"]) for log in repo.list_logs()}
    for code in expected:
        for phase in ("开始", "完成"):
            if (code, phase) not in phases:
                problems.append(f"回填日志缺少 {code} 的「{phase}」记录")

    pl1 = repo.get_node("PL-0001")
    pl2 = repo.get_node("PL-0002")
    pl3 = repo.get_node("PL-0003")
    ok = not problems
    _print(
        {
            "ok": ok,
            "checks": {
                "node_count": len(repo.list_nodes()),
                "edge_count": len(repo.list_edges()),
                "snapshot_count": len(repo.list_snapshots()),
                "pl1_completed": bool(pl1) and pl1["runtime_status"] == STATUS_COMPLETED,
                "pl2_failed": bool(pl2) and pl2["runtime_status"] == STATUS_FAILED,
                "pl3_waiting": bool(pl3) and pl3["runtime_status"] == STATUS_WAITING,
            },
            "problems": problems,
        }
    )
    return 0 if ok else 1


def cmd_reset(args: argparse.Namespace) -> int:
    repo = _build_repo(args)
    repo.reset()
    _print({"ok": True, "message": "管线持久层已清空", "db_path": str(repo.db_path)})
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="产品管线运行状态迁移工具")
    parser.add_argument("--db-path", default=None, help="覆盖 PIPELINE_DB_PATH")
    parser.add_argument("--log-dir", default=None, help="覆盖 PIPELINE_MIGRATION_LOG_DIR")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="建库建表并回填既有示例节点").set_defaults(func=cmd_init)

    migrate = sub.add_parser("migrate", help="执行或断点续跑迁移")
    migrate.add_argument(
        "--interrupt-after",
        default=None,
        help="演练用：在指定节点开始后中断（如 PL-0002）",
    )
    migrate.set_defaults(func=cmd_migrate)

    sub.add_parser("status", help="输出持久层完整状态").set_defaults(func=cmd_status)
    sub.add_parser("verify", help="按验收口径核对迁移结果").set_defaults(func=cmd_verify)
    sub.add_parser("reset", help="清空管线持久层").set_defaults(func=cmd_reset)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
