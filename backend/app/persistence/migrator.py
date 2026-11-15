"""产品管线运行结果与节点依赖状态迁移器。

迁移样例（按节点编号排序）：

- PL-0001：已完成，重试策略 0 次，输出格式 parquet
- PL-0002：失败（失败码 ORA-LOAD-17），重试策略 3 次，输出格式 parquet
- PL-0003：等待上游，重试策略 2 次，输出格式 parquet

依赖关系：PL-0001 → PL-0002 → PL-0003。

并发与中断语义：

1. 迁移全程持进程内锁，新加入节点与回填并发时由仓储的唯一约束兜底——
   先完成落库的节点/依赖边生效，后到写入忽略，绝不产生第二条依赖边。
2. 每个节点落库成功后写断点，回填中断后重启从「最后一个完整节点之后」继续，
   已完成的节点不会被二次处理。
3. PL-0003 的历史状态是「等待上游」，迁移器只做历史状态恢复，绝不把等待改成运行中；
   运行态转换统一走 :meth:`PipelineRepository.mark_running` 的前置完成校验。
"""
from __future__ import annotations

import threading
from typing import Any

from app.persistence.repository import (
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_WAITING,
    PipelineRepository,
    get_repo,
)

# 输出格式快照在迁移样例里统一为 parquet
SAMPLE_OUTPUT_FORMAT = "parquet"

# 迁移样例：节点编号、运行结果、重试策略、失败码
PIPELINE_SAMPLES: list[dict[str, Any]] = [
    {
        "node_code": "PL-0001",
        "node_name": "产品管线-入湖装载节点",
        "upstream_code": None,
        "runtime_status": STATUS_COMPLETED,
        "retry_policy": 0,
        "retry_used": 0,
        "failure_code": None,
        "output_format": SAMPLE_OUTPUT_FORMAT,
    },
    {
        "node_code": "PL-0002",
        "node_name": "产品管线-加工转换节点",
        "upstream_code": "PL-0001",
        "runtime_status": STATUS_FAILED,
        "retry_policy": 3,
        "retry_used": 3,
        "failure_code": "ORA-LOAD-17",
        "output_format": SAMPLE_OUTPUT_FORMAT,
    },
    {
        "node_code": "PL-0003",
        "node_name": "产品管线-发布输出节点",
        "upstream_code": "PL-0002",
        "runtime_status": STATUS_WAITING,
        "retry_policy": 2,
        "retry_used": 0,
        "failure_code": None,
        "output_format": SAMPLE_OUTPUT_FORMAT,
    },
]

# 依赖关系 PL-0001 → PL-0002 → PL-0003
DEPENDENCY_EDGES: list[tuple[str, str]] = [
    ("PL-0001", "PL-0002"),
    ("PL-0002", "PL-0003"),
]


class MigrationInterrupted(RuntimeError):
    """回填被外部中断（测试或演练用），断点保留在最后一个完整节点。"""


class PipelineMigrator:
    """按节点编号驱动的幂等迁移器，可反复执行、可断点续跑。"""

    _run_lock = threading.RLock()

    def __init__(self, repo: PipelineRepository | None = None) -> None:
        self.repo = repo or get_repo()

    # ------------------------------------------------------------ 回填入口
    def run(
        self,
        *,
        interrupt_after: str | None = None,
        scope: str = "default",
        extra_nodes: list[dict[str, Any]] | None = None,
        extra_edges: list[tuple[str, str]] | None = None,
    ) -> dict[str, Any]:
        """执行一轮回填。

        - ``interrupt_after``：在该节点「开始之后、完成之前」抛 :class:`MigrationInterrupted`，
          用来演练中断续跑；断点保持在上一个完整节点。
        - ``extra_nodes`` / ``extra_edges``：新加入节点与既有节点并发回填时走同一条落库路径，
          由仓储唯一约束保证先落库者为准、依赖边不重复。
        """
        with self._run_lock:
            checkpoint = self.repo.get_checkpoint(scope)
            done_codes: set[str] = set()
            if checkpoint and checkpoint.get("last_completed_node"):
                done_codes.add(str(checkpoint["last_completed_node"]))
            # 断点表里只记录最后一个完整节点；节点编号顺序保证它之前的节点都完整。
            ordered_codes = [str(sample["node_code"]) for sample in PIPELINE_SAMPLES]
            if done_codes:
                last = next(iter(done_codes))
                if last in ordered_codes:
                    done_codes.update(ordered_codes[: ordered_codes.index(last) + 1])

            pending = [
                sample
                for sample in PIPELINE_SAMPLES
                if str(sample["node_code"]) not in done_codes
            ]

            processed: list[str] = []
            skipped: list[str] = []
            interrupted_at: str | None = None

            # 新加入节点与样例节点统一按节点编号排序后落库，保证顺序确定。
            all_nodes = sorted(
                pending + list(extra_nodes or []), key=lambda item: str(item["node_code"])
            )
            for node in all_nodes:
                code = str(node["node_code"])
                # 断点之外仍要防并发：仓储里已存在的节点直接跳过，先落库者为准。
                if self.repo.get_node(code) is not None:
                    skipped.append(code)
                    continue

                self.repo.add_log(code, "开始", f"节点 {code} 回填开始")
                if interrupt_after == code:
                    interrupted_at = code
                    self.repo.add_log(
                        code,
                        "中断",
                        f"节点 {code} 回填中断，断点保留在上一个完整节点",
                    )
                    raise MigrationInterrupted(
                        f"回填在节点 {code} 处中断，重启后从该节点继续"
                    )

                row, created = self.repo.upsert_node(node)
                if not created:
                    skipped.append(code)
                    self.repo.add_log(code, "跳过", f"节点 {code} 已落库，先落库记录为准")
                    continue

                self.repo.upsert_snapshot(
                    code,
                    str(node.get("output_format") or SAMPLE_OUTPUT_FORMAT),
                    detail={
                        "node_code": code,
                        "node_name": row["node_name"],
                        "output_format": row["output_format"],
                        "runtime_status": row["runtime_status"],
                    },
                )
                self.repo.save_checkpoint(code, scope=scope)
                processed.append(code)
                self.repo.add_log(
                    code,
                    "完成",
                    f"节点 {code} 回填完成，状态={row['runtime_status']}，"
                    f"输出格式={row['output_format']}",
                )

            # 依赖边在两端节点都落库后统一写入；INSERT OR IGNORE 保证重跑不产生第二条边。
            edge_written: list[str] = []
            edge_deduped: list[str] = []
            for upstream, downstream in list(DEPENDENCY_EDGES) + list(extra_edges or []):
                if (
                    self.repo.get_node(upstream) is None
                    or self.repo.get_node(downstream) is None
                ):
                    # 中断导致端点未落库时跳过，续跑轮次会补上。
                    continue
                created = self.repo.upsert_edge(upstream, downstream)
                label = f"{upstream}->{downstream}"
                (edge_written if created else edge_deduped).append(label)

            return {
                "processed": processed,
                "skipped": skipped,
                "edges_written": edge_written,
                "edges_deduped": edge_deduped,
                "interrupted_at": interrupted_at,
                "checkpoint": (self.repo.get_checkpoint(scope) or {}).get("last_completed_node"),
            }

    # ------------------------------------------------------------ 种子回填
    def backfill_seed_nodes(self, seed_rows: list[dict[str, Any]]) -> dict[str, Any]:
        """把既有示例管线节点（PIPE 编号）幂等回填到持久层。

        旧内存仓库里的示例节点字段是占位文本，这里只补齐持久层缺失的重试策略与
        输出格式快照；节点编号已存在时整体跳过，兼容旧依赖与既有节点，不覆盖任何记录。
        """
        backfilled: list[str] = []
        skipped: list[str] = []
        for row in sorted(seed_rows, key=lambda item: str(item.get("节点编号", ""))):
            code = str(row.get("节点编号") or "").strip()
            if not code:
                continue
            if self.repo.get_node(code) is not None:
                skipped.append(code)
                continue
            retry_policy = _coerce_int(row.get("重试策略"), default=0)
            output_format = str(row.get("输出格式") or "").strip() or SAMPLE_OUTPUT_FORMAT
            node = {
                "node_code": code,
                "node_name": str(row.get("节点名称") or code),
                "upstream_code": _parse_upstream(row.get("上游节点")),
                # 旧示例没有运行时口径，统一按等待上游落库，等待态不会被启动流程提前改写。
                "runtime_status": STATUS_WAITING,
                "retry_policy": retry_policy,
                "retry_used": 0,
                "failure_code": None,
                "output_format": output_format,
            }
            self.repo.upsert_node(node)
            self.repo.upsert_snapshot(
                code,
                output_format,
                detail={
                    "node_code": code,
                    "node_name": node["node_name"],
                    "output_format": output_format,
                    "source": "seed",
                },
            )
            backfilled.append(code)
        return {"backfilled": backfilled, "skipped": skipped}


def _coerce_int(value: Any, *, default: int) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _parse_upstream(value: Any) -> str | None:
    """旧示例的上游节点是占位文本，只有形如节点编号的值才作为依赖边来源。"""
    text = str(value or "").strip()
    return text if "-" in text and text[:1].isalpha() else None
