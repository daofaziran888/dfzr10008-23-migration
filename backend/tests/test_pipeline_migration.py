"""产品管线运行状态迁移的验收测试。

覆盖：
1. 完整迁移后 PL-0001/0002/0003 的状态、重试策略、parquet 输出格式快照与 ORA-LOAD-17 失败码；
2. 重启（重新连接同一 sqlite 文件）后节点、依赖边、快照完整恢复；
3. 回填中断后从最后一个完整节点（PL-0002）继续，等待中的 PL-0003 不提前变成运行中；
4. 重复执行不产生第二条依赖边；
5. 新加入节点与回填并发时先完成落库者为准；
6. 数据模型封装按 PL-0001 仍能读到原 parquet 格式；
7. 回填日志按节点编号记录开始、完成和中断位置。
"""
from __future__ import annotations

import sys
import tempfile
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.persistence.migrator import (  # noqa: E402
    DEPENDENCY_EDGES,
    MigrationInterrupted,
    PipelineMigrator,
)
from app.persistence.repository import (  # noqa: E402
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_RUNNING,
    STATUS_WAITING,
    PipelineRepository,
    configure,
)
from app.seed import SEED_ROWS  # noqa: E402
from app.services.datapack import DatapackService  # noqa: E402


class MigrationTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.workdir = Path(self._tmp.name)
        # configure 同时替换进程级单例，保证服务层（如 DatapackService）读到同一套临时仓储
        self.repo = configure(
            db_path=str(self.workdir / "state.sqlite3"),
            log_dir=str(self.workdir / "logs"),
        )
        self.migrator = PipelineMigrator(self.repo)

    def reopen_repo(self) -> PipelineRepository:
        """模拟进程重启：关闭连接后用同一数据文件重新打开仓储。"""
        self.repo.close()
        self.repo = configure(
            db_path=str(self.workdir / "state.sqlite3"),
            log_dir=str(self.workdir / "logs"),
        )
        self.migrator = PipelineMigrator(self.repo)
        return self.repo


class FullMigrationTests(MigrationTestBase):
    def test_full_migration_restores_states_retries_and_snapshots(self) -> None:
        self.migrator.backfill_seed_nodes(SEED_ROWS["pipeline"])
        result = self.migrator.run()

        self.assertEqual(result["processed"], ["PL-0001", "PL-0002", "PL-0003"])

        pl1 = self.repo.get_node("PL-0001")
        pl2 = self.repo.get_node("PL-0002")
        pl3 = self.repo.get_node("PL-0003")
        self.assertIsNotNone(pl1)
        self.assertIsNotNone(pl2)
        self.assertIsNotNone(pl3)
        assert pl1 and pl2 and pl3

        self.assertEqual(pl1["runtime_status"], STATUS_COMPLETED)
        self.assertEqual(pl2["runtime_status"], STATUS_FAILED)
        self.assertEqual(pl3["runtime_status"], STATUS_WAITING)

        self.assertEqual(pl1["retry_policy"], 0)
        self.assertEqual(pl2["retry_policy"], 3)
        self.assertEqual(pl3["retry_policy"], 2)

        # 迁移核对：PL-0002 的失败码必须是 ORA-LOAD-17
        self.assertEqual(pl2["failure_code"], "ORA-LOAD-17")
        self.assertIsNone(pl1["failure_code"])
        self.assertIsNone(pl3["failure_code"])

        for code in ("PL-0001", "PL-0002", "PL-0003"):
            self.assertEqual(self.repo.get_node(code)["output_format"], "parquet")
            snapshot = self.repo.get_snapshot(code)
            self.assertIsNotNone(snapshot)
            assert snapshot
            self.assertEqual(snapshot["output_format"], "parquet")

        edges = self.repo.list_edges()
        self.assertEqual(
            {(e["upstream_node"], e["downstream_node"]) for e in edges},
            set(DEPENDENCY_EDGES),
        )
        self.assertEqual(len(edges), 2)

        checkpoint = self.repo.get_checkpoint()
        self.assertIsNotNone(checkpoint)
        assert checkpoint
        self.assertEqual(checkpoint["last_completed_node"], "PL-0003")

    def test_state_survives_restart(self) -> None:
        self.migrator.run()
        self.reopen_repo()

        nodes = {row["node_code"]: row for row in self.repo.list_nodes()}
        self.assertEqual(nodes["PL-0001"]["runtime_status"], STATUS_COMPLETED)
        self.assertEqual(nodes["PL-0002"]["runtime_status"], STATUS_FAILED)
        self.assertEqual(nodes["PL-0002"]["failure_code"], "ORA-LOAD-17")
        self.assertEqual(nodes["PL-0003"]["runtime_status"], STATUS_WAITING)

        self.assertEqual(len(self.repo.list_edges()), 2)
        self.assertEqual(self.repo.get_snapshot("PL-0001")["output_format"], "parquet")
        # 重启后日志仍可按节点编号读取
        pl2_logs = self.repo.list_logs("PL-0002")
        self.assertIn("开始", {log["phase"] for log in pl2_logs})
        self.assertIn("完成", {log["phase"] for log in pl2_logs})

    def test_datapack_service_reads_original_format_for_pl0001(self) -> None:
        self.migrator.run()
        service = DatapackService()
        snapshot = service.get_output_snapshot("PL-0001")
        self.assertIsNotNone(snapshot)
        assert snapshot
        self.assertEqual(snapshot["output_format"], "parquet")
        self.assertEqual(snapshot["detail"]["node_code"], "PL-0001")


class InterruptAndResumeTests(MigrationTestBase):
    def test_interrupt_resumes_from_last_complete_node(self) -> None:
        # 在 PL-0002 开始后中断：PL-0001 是最后一个完整节点
        with self.assertRaises(MigrationInterrupted):
            self.migrator.run(interrupt_after="PL-0002")

        self.assertIsNotNone(self.repo.get_node("PL-0001"))
        self.assertIsNone(self.repo.get_node("PL-0002"))
        self.assertIsNone(self.repo.get_node("PL-0003"))
        checkpoint = self.repo.get_checkpoint()
        assert checkpoint
        self.assertEqual(checkpoint["last_completed_node"], "PL-0001")

        # 中断位置写进回填日志
        interrupt_logs = [
            log for log in self.repo.list_logs("PL-0002") if log["phase"] == "中断"
        ]
        self.assertEqual(len(interrupt_logs), 1)

        # 模拟重启后续跑：从 PL-0002 开始
        self.reopen_repo()
        result = self.migrator.run()
        self.assertEqual(result["processed"], ["PL-0002", "PL-0003"])

        pl2 = self.repo.get_node("PL-0002")
        pl3 = self.repo.get_node("PL-0003")
        assert pl2 and pl3
        self.assertEqual(pl2["runtime_status"], STATUS_FAILED)
        self.assertEqual(pl2["failure_code"], "ORA-LOAD-17")
        self.assertEqual(pl3["runtime_status"], STATUS_WAITING)
        self.assertEqual(len(self.repo.list_edges()), 2)

    def test_repeated_run_does_not_duplicate_edges_or_nodes(self) -> None:
        self.migrator.run()
        again = self.migrator.run()
        self.assertEqual(again["processed"], [])
        self.assertEqual(len(self.repo.list_edges()), 2)
        self.assertEqual(len(self.repo.list_nodes()), 3)
        # 第三次（模拟再重启）仍然幂等
        self.reopen_repo()
        third = self.migrator.run()
        self.assertEqual(third["processed"], [])
        self.assertEqual(len(self.repo.list_edges()), 2)

    def test_waiting_node_cannot_become_running_before_upstream_done(self) -> None:
        self.migrator.run()
        # PL-0003 前置 PL-0002 是失败态，未完成：启动请求必须被拦下
        node, message = self.repo.mark_running("PL-0003")
        assert node
        self.assertEqual(node["runtime_status"], STATUS_WAITING)
        self.assertIn("等待上游", message)

        # 迁移器再跑一轮也不能改写等待态
        self.migrator.run()
        self.assertEqual(self.repo.get_node("PL-0003")["runtime_status"], STATUS_WAITING)

        # 前置补齐为已完成后才解除等待
        self.repo.update_status("PL-0002", STATUS_COMPLETED)
        running, ok_message = self.repo.mark_running("PL-0003")
        assert running
        self.assertEqual(running["runtime_status"], "运行中")
        self.assertIn("运行中", ok_message)


class ConcurrencyTests(MigrationTestBase):
    def test_new_node_concurrent_backfill_first_writer_wins(self) -> None:
        # 新节点 PL-0004 与样例回填并发：两条线程都尝试写同一编号与同一条边
        new_node = {
            "node_code": "PL-0004",
            "node_name": "并发新增节点",
            "upstream_code": "PL-0003",
            "runtime_status": STATUS_WAITING,
            "retry_policy": 1,
            "retry_used": 0,
            "failure_code": None,
            "output_format": "parquet",
        }
        extra_edges = [("PL-0003", "PL-0004")]
        errors: list[Exception] = []

        def worker() -> None:
            try:
                self.migrator.run(extra_nodes=[dict(new_node)], extra_edges=extra_edges)
            except Exception as exc:  # pragma: no cover - 失败时在主线程断言
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])
        node = self.repo.get_node("PL-0004")
        self.assertIsNotNone(node)
        assert node
        self.assertEqual(node["retry_policy"], 1)
        # 依赖边只有一条，不产生重复
        edges = [
            edge
            for edge in self.repo.list_edges()
            if edge["upstream_node"] == "PL-0003" and edge["downstream_node"] == "PL-0004"
        ]
        self.assertEqual(len(edges), 1)

    def test_upsert_node_first_writer_wins_keeps_original(self) -> None:
        row, created = self.repo.upsert_node(
            {
                "node_code": "PL-0009",
                "node_name": "先落库",
                "runtime_status": STATUS_COMPLETED,
                "retry_policy": 5,
                "output_format": "parquet",
            }
        )
        self.assertTrue(created)
        again, created_again = self.repo.upsert_node(
            {
                "node_code": "PL-0009",
                "node_name": "后到的覆盖尝试",
                "runtime_status": STATUS_RUNNING,
                "retry_policy": 9,
                "output_format": "json",
            }
        )
        self.assertFalse(created_again)
        self.assertEqual(again["node_name"], "先落库")
        self.assertEqual(again["retry_policy"], 5)
        self.assertEqual(again["output_format"], "parquet")


class SeedBackfillTests(MigrationTestBase):
    def test_seed_nodes_backfilled_idempotently_with_snapshots(self) -> None:
        first = self.migrator.backfill_seed_nodes(SEED_ROWS["pipeline"])
        second = self.migrator.backfill_seed_nodes(SEED_ROWS["pipeline"])

        self.assertEqual(len(first["backfilled"]), 3)
        self.assertEqual(second["backfilled"], [])
        self.assertEqual(set(second["skipped"]), {"PIPE-0001", "PIPE-0002", "PIPE-0003"})
        self.assertEqual(len(self.repo.list_nodes()), 3)

        snapshot = self.repo.get_snapshot("PIPE-0001")
        self.assertIsNotNone(snapshot)
        # 旧示例占位文本不是合法重试次数时回退为 0
        pipe1 = self.repo.get_node("PIPE-0001")
        assert pipe1
        self.assertEqual(pipe1["retry_policy"], 0)
        self.assertEqual(pipe1["runtime_status"], STATUS_WAITING)

    def test_migration_log_file_records_phases_per_node(self) -> None:
        self.migrator.run()
        content = self.repo.log_file.read_text(encoding="utf-8")
        for code in ("PL-0001", "PL-0002", "PL-0003"):
            self.assertIn(f"节点={code} | 阶段=开始", content)
            self.assertIn(f"节点={code} | 阶段=完成", content)


if __name__ == "__main__":
    unittest.main()
