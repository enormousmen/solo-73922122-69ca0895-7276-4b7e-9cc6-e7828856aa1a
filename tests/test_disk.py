"""disk.py 的单元/集成测试（不依赖 Flask，可直接 python -m unittest）。

运行：python -m unittest discover -s tests -v
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.disk import (BLOCK_SIZE, DATA_OFFSET, HEADER_SIZE, MAX_BATCH,
                      STATE_COMPACTING, STATE_ERROR, STATE_IDLE, Disk,
                      DiskError, _pack_header, _unpack_header)
from app.seed import SEED_LAYOUT, block_content, build as build_seed

# 必须借空槽周转的布局：槽 0 被在用块占着，且块 0 在后面的槽里。
PIVOT_LAYOUT = [3, 0, None, 1, None, 2]
# 含至少 4 个连续最终空槽的布局：首批必须合到 MAX_BATCH=4 块。
BATCH4_LAYOUT = [
    None, None, None, None, 0, 1, 2, 3,
    None, 4, 5,
]
# 最终槽 0..2 全空、但没有第 4 个连续空槽：首批应被占槽截断为 3 块。
BATCH3_LAYOUT = [None, None, None, 5, 0, 1, 2, 3, 4]


def build_custom(path: str, layout: list[int | None]) -> None:
    """按给定物理布局建镜像：内容仍用确定性 block_content(lbn)。"""
    build_seed(path, layout=layout)


class DiskTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.img = os.path.join(self.tmp.name, "disk.img")
        build_seed(self.img)
        self.expected = {lbn: block_content(lbn)
                         for lbn in SEED_LAYOUT if lbn is not None}

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def assertAllBlocksReadable(self, disk: Disk) -> None:
        for lbn, content in self.expected.items():
            self.assertEqual(disk.read_lbn(lbn), content,
                             f"lbn {lbn} 内容被破坏")

    def open(self) -> Disk:
        return Disk(self.img)


class TestSeedAndRead(DiskTestBase):
    def test_seed_layout(self):
        disk = self.open()
        st = disk.status()
        self.assertEqual(st["slot_count"], 20)
        self.assertEqual(st["used_count"], 14)
        self.assertEqual(st["used_lbns"], list(range(14)))
        self.assertEqual(st["file_size"], DATA_OFFSET + 20 * BLOCK_SIZE)
        self.assertAllBlocksReadable(disk)
        disk.close()

    def test_read_missing_lbn(self):
        disk = self.open()
        with self.assertRaises(KeyError):
            disk.read_lbn(99)
        disk.close()

    def test_file_size_holds_tail(self):
        # 整理前：尾部还有空槽，文件仍按全部 20 槽占位。
        disk = self.open()
        self.assertGreater(disk.file_size, disk.expected_compact_size())
        disk.close()


class TestBatchCompaction(DiskTestBase):
    def test_full_compaction_batches_and_stats(self):
        # 用每轮 moves 差值记录每批实际块数。
        os.remove(self.img)
        build_seed(self.img)
        disk = self.open()
        disk.start_compaction()
        sizes: list[int] = []
        last_moves = 0
        while True:
            done = disk.compact_once()
            sizes.append(disk.moves - last_moves)
            last_moves = disk.moves
            if done:
                break
        st = disk.status()
        self.assertEqual(st["state"], STATE_IDLE)
        # 演示镜像的批次序列：4 + 4 + 4 + 2，最后一次调用只截短不搬块。
        self.assertEqual(sizes, [4, 4, 4, 2, 0])
        self.assertLessEqual(max(sizes), MAX_BATCH)
        self.assertEqual(st["max_batch_committed"], 4)
        self.assertEqual(st["max_batch_capacity"], MAX_BATCH)
        # 14 次搬移分 4 批：每批数据+盘头各 1 次 fsync（8 次）；
        # 截短 1 次 + 截短后的 idle 盘头 1 次（2 次）。
        # 启动整理的那次提交发生在计数置位之前，不计入本轮。
        self.assertEqual(st["fsyncs"], 4 * 2 + 2)
        # 文件长度恰好容纳头 + 全部在用块。
        self.assertEqual(disk.file_size,
                         DATA_OFFSET + 14 * BLOCK_SIZE)
        self.assertEqual(disk.file_size, disk.expected_compact_size())
        self.assertEqual(st["slots"][:14], [i + 1 for i in range(14)])
        self.assertEqual(set(st["slots"][14:]), {0})
        self.assertAllBlocksReadable(disk)
        self.assertLessEqual(st["max_temp_bytes"], BLOCK_SIZE)
        disk.close()

    def test_each_move_batch_uses_at_most_two_fsyncs(self):
        disk = self.open()
        disk.start_compaction()
        last = disk.fsyncs
        while True:
            done = disk.compact_once()
            delta = disk.fsyncs - last
            if not done:
                # 搬移批固定 2 次 fsync（数据一次、盘头一次）。
                self.assertEqual(delta, 2)
            last = disk.fsyncs
            if done:
                break
        disk.close()

    def test_idempotent_compact_resets_round_stats(self):
        disk = self.open()
        disk.start_compaction()
        while not disk.compact_once():
            pass
        moves_before = disk.moves
        self.assertEqual(disk.max_batch, 4)
        # 新一轮整理：统计清零，且压紧态不产生任何搬移。
        disk.start_compaction()
        self.assertEqual(disk.moves, 0)
        self.assertEqual(disk.fsyncs, 0)
        self.assertEqual(disk.max_batch, 0)
        while not disk.compact_once():
            pass
        self.assertEqual(disk.moves, 0)
        self.assertEqual(disk.file_size, DATA_OFFSET + 14 * BLOCK_SIZE)
        # 压紧轮次没有搬移批、也没有游标提交：
        # 只有截短 fsync 与 idle 盘头提交共 2 次。
        self.assertEqual(disk.fsyncs, 2)
        disk.close()

    def test_temp_never_exceeds_one_block(self):
        disk = self.open()
        disk.start_compaction()
        while not disk.compact_once():
            self.assertLessEqual(disk.max_temp_seen, BLOCK_SIZE)
        self.assertLessEqual(disk.max_temp_seen, BLOCK_SIZE)
        disk.close()


class TestBatchPlanner(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def _open(self, layout):
        img = os.path.join(self.tmp.name, f"d_{len(layout)}.img")
        build_custom(img, layout)
        disk = Disk(img)
        disk.start_compaction()
        return disk

    def test_first_batch_caps_at_four(self):
        disk = self._open(BATCH4_LAYOUT)
        moves, new_cursor, pivot = disk._plan_batch()  # noqa: SLF001
        self.assertFalse(pivot)
        self.assertEqual(len(moves), 4)
        # 目标槽恰好是最终空槽 0..3。
        self.assertEqual(sorted(dst for _, dst in moves), [0, 1, 2, 3])
        self.assertEqual(new_cursor, 4)
        disk.close()

    def test_batch_stops_at_occupied_final_slot(self):
        disk = self._open(BATCH3_LAYOUT)
        moves, new_cursor, pivot = disk._plan_batch()  # noqa: SLF001
        self.assertFalse(pivot)
        self.assertEqual(len(moves), 3)
        self.assertEqual(sorted(dst for _, dst in moves), [0, 1, 2])
        self.assertEqual(new_cursor, 3)
        disk.close()

    def test_pivot_layout_compacts_with_temp_slot(self):
        img = os.path.join(self.tmp.name, "pivot.img")
        build_custom(img, PIVOT_LAYOUT)
        disk = Disk(img)
        used = sorted(l for l in PIVOT_LAYOUT if l is not None)
        disk.start_compaction()
        # 首批必须是 1 块枢轴批：把占槽 0 的块 3 挪到空槽。
        moves, new_cursor, pivot = disk._plan_batch()  # noqa: SLF001
        self.assertTrue(pivot)
        self.assertEqual(len(moves), 1)
        self.assertEqual(moves[0][0], 0)
        self.assertEqual(new_cursor, 0)
        pivot_dst = moves[0][1]
        # 跑完整轮：最终仍按逻辑块号压紧、截短、内容不变。
        while not disk.compact_once():
            pass
        st = disk.status()
        self.assertEqual(st["state"], STATE_IDLE)
        self.assertEqual(st["slots"][:len(used)],
                         [l + 1 for l in range(len(used))])
        self.assertEqual(disk.file_size,
                         DATA_OFFSET + len(used) * BLOCK_SIZE)
        for lbn in used:
            self.assertEqual(disk.read_lbn(lbn), block_content(lbn))
        # 枢轴批同样只花 2 次 fsync。
        self.assertLessEqual(st["max_batch_committed"], MAX_BATCH)
        self.assertGreaterEqual(st["fsyncs"], 2)
        disk.close()

    def test_no_batch_exceeds_four_moves(self):
        for layout in (SEED_LAYOUT, BATCH4_LAYOUT, BATCH3_LAYOUT,
                       PIVOT_LAYOUT):
            img = os.path.join(self.tmp.name, f"r_{id(layout)}.img")
            build_custom(img, layout)
            disk = Disk(img)
            disk.start_compaction()
            while not disk.compact_once():
                self.assertLessEqual(disk.max_batch, MAX_BATCH)
            disk.close()

    def test_fully_allocated_volume_reports_disk_error(self):
        # 没有任何空槽时无法借枢轴周转：明确报 DiskError，而不是乱搬。
        img = os.path.join(self.tmp.name, "full.img")
        build_custom(img, [1, 0])
        disk = Disk(img)
        disk.start_compaction()
        with self.assertRaises(DiskError):
            disk.compact_once()
        disk.close()

    def test_planned_slots_are_disjoint_and_dests_free(self):
        img = os.path.join(self.tmp.name, "plan.img")
        build_custom(img, BATCH4_LAYOUT)
        disk = Disk(img)
        disk.start_compaction()
        while disk.state == "compacting":
            moves, _cur, _pivot = disk._plan_batch()  # noqa: SLF001
            if not moves:
                break
            srcs = [s for s, _ in moves]
            dsts = [d for _, d in moves]
            self.assertEqual(len(srcs + dsts), len(set(srcs + dsts)))
            for _, dst in moves:
                self.assertEqual(disk.slots[dst], 0)
            disk.compact_once()
        disk.close()


class TestFaultsAndRecovery(DiskTestBase):
    def test_pre_io_move_error_changes_nothing(self):
        disk = self.open()
        disk.start_compaction()
        slots_before = list(disk.slots)
        gen_at_start = disk._gen  # noqa: SLF001
        disk.inject_move_error = True
        with self.assertRaises(RuntimeError):
            disk.compact_once()
        disk.record_error()
        self.assertEqual(disk.status()["state"], STATE_ERROR)
        # 任何 I/O 之前就报错：槽表、游标均不变；只有 record_error 的
        # 错误态提交推进了世代号。
        self.assertEqual(disk.slots, slots_before)
        self.assertEqual(disk._gen, gen_at_start + 1)  # noqa: SLF001
        self.assertEqual(disk.cursor, 0)
        self.assertAllBlocksReadable(disk)
        disk.close()

        disk = self.open()
        self.assertEqual(disk.status()["state"], STATE_ERROR)
        disk.start_compaction()
        while not disk.compact_once():
            pass
        self.assertEqual(disk.status()["state"], STATE_IDLE)
        self.assertEqual(disk.file_size, DATA_OFFSET + 14 * BLOCK_SIZE)
        self.assertAllBlocksReadable(disk)
        disk.close()

    def test_mid_batch_data_write_error_leaves_pre_batch_layout(self):
        disk = self.open()
        disk.start_compaction()
        gen_at_start = disk._gen  # noqa: SLF001
        # 让首批（4 块）在第 2 次数据写时失败。
        disk.inject_data_write_error_at = 2
        slots_before = list(disk.slots)
        cursor_before = disk.cursor
        with self.assertRaises(RuntimeError):
            disk.compact_once()
        disk.record_error()
        # 槽表未提交：内存与盘上头都还是批前布局，进度不前进。
        self.assertEqual(disk.slots, slots_before)
        self.assertEqual(disk.cursor, cursor_before)
        self.assertEqual(disk.moves, 0)
        self.assertEqual(disk.max_batch, 0)
        self.assertAllBlocksReadable(disk)
        disk.close()

        # 重开确认盘上也是批前布局（只看到整批搬移前的状态）。
        disk = self.open()
        self.assertEqual(disk.slots, slots_before)
        # 批处理本身没有提交盘头；世代号只被 record_error 推进了一次。
        self.assertEqual(disk._gen, gen_at_start + 1)  # noqa: SLF001
        self.assertEqual(disk.status()["state"], STATE_ERROR)
        disk.start_compaction()
        while not disk.compact_once():
            pass
        st = disk.status()
        self.assertEqual(st["state"], STATE_IDLE)
        self.assertEqual(disk.file_size, DATA_OFFSET + 14 * BLOCK_SIZE)
        # 续作统计不重置：首轮失败没计数，成功轮次从 0 重新累计。
        self.assertEqual(st["max_batch_committed"], 4)
        self.assertAllBlocksReadable(disk)
        disk.close()

    def test_truncate_error_keeps_data_and_resumes(self):
        disk = self.open()
        disk.start_compaction()
        disk.inject_truncate_error = True
        while True:
            try:
                done = disk.compact_once()
            except RuntimeError:
                disk.record_error()
                break
            if done:
                break
        self.assertEqual(disk.status()["state"], STATE_ERROR)
        self.assertAllBlocksReadable(disk)
        self.assertEqual(disk.file_size, DATA_OFFSET + 20 * BLOCK_SIZE)
        disk.close()

        disk = self.open()
        disk.start_compaction()
        while not disk.compact_once():
            pass
        self.assertEqual(disk.file_size, DATA_OFFSET + 14 * BLOCK_SIZE)
        self.assertAllBlocksReadable(disk)
        disk.close()

    def test_fsync_counter_survives_error_resume(self):
        disk = self.open()
        disk.start_compaction()
        disk.inject_move_error = True
        with self.assertRaises(RuntimeError):
            disk.compact_once()
        disk.record_error()
        disk.close()
        disk = self.open()
        fsync_before = disk.fsyncs
        disk.start_compaction()  # error -> compacting 不重置统计
        self.assertEqual(disk.fsyncs, fsync_before + 1)
        while not disk.compact_once():
            pass
        self.assertEqual(disk.status()["state"], STATE_IDLE)
        disk.close()


class TestConcurrentReads(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.img = os.path.join(self.tmp.name, "disk.img")
        build_seed(self.img)
        self.expected = {lbn: block_content(lbn)
                         for lbn in SEED_LAYOUT if lbn is not None}

    def tearDown(self):
        self.tmp.cleanup()

    def test_reads_during_batched_compaction_always_complete(self):
        disk = Disk(self.img)
        disk.move_delay = 0.02
        errors: list[Exception] = []
        stop = threading.Event()

        def reader():
            while not stop.is_set():
                lbn = threading.get_ident() % 14
                try:
                    data = disk.read_lbn(lbn)
                    if data != self.expected[lbn]:
                        errors.append(AssertionError(f"lbn {lbn} 数据错误"))
                except Exception as exc:  # 整理期间任何异常都算失败
                    errors.append(exc)
                time.sleep(0.001)

        readers = [threading.Thread(target=reader) for _ in range(6)]
        disk.start_compaction()
        for t in readers:
            t.start()
        while not disk.compact_once():
            pass
        stop.set()
        for t in readers:
            t.join(timeout=5)
        self.assertEqual(errors, [])
        for lbn, content in self.expected.items():
            self.assertEqual(disk.read_lbn(lbn), content)
        disk.close()


class TestCrashRecovery(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.img = os.path.join(self.tmp.name, "disk.img")
        build_seed(self.img)

    def tearDown(self):
        self.tmp.cleanup()

    def _helper(self) -> str:
        return os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "_crash_worker.py")

    def _env(self) -> dict:
        return dict(os.environ, PYTHONPATH=os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))))

    def test_kill_mid_compaction_then_resume(self):
        helper = self._helper()
        # 第一轮：工作进程完整提交一批后 os._exit 硬退出。
        run1 = subprocess.run(
            [sys.executable, helper, self.img, "compact-hard"],
            env=self._env(), capture_output=True, text=True, timeout=30)
        self.assertEqual(run1.returncode, 42)
        self.assertIn("COMPACT_STARTED", run1.stdout)
        disk = Disk(self.img)
        self.assertEqual(disk.status()["state"], STATE_COMPACTING)
        # 首批 4 块已提交：游标=4，moves=4，最大批量=4。
        self.assertEqual(disk.cursor, 4)
        self.assertEqual(disk.moves, 4)
        self.assertEqual(disk.max_batch, 4)
        for lbn in range(14):
            self.assertEqual(disk.read_lbn(lbn), block_content(lbn))
        disk.close()

        # 第二轮：再次硬退出，验证多轮中断后仍可续作。
        run2 = subprocess.run(
            [sys.executable, helper, self.img, "compact-hard"],
            env=self._env(), capture_output=True, text=True, timeout=30)
        self.assertEqual(run2.returncode, 42)
        disk = Disk(self.img)
        self.assertEqual(disk.status()["state"], STATE_COMPACTING)
        self.assertEqual(disk.cursor, 8)
        disk.close()

        # 第三轮：新进程续作直到完成。
        out = subprocess.run(
            [sys.executable, helper, self.img, "resume"],
            env=self._env(), check=True, capture_output=True,
            text=True, timeout=30)
        self.assertIn("state=idle", out.stdout)
        disk = Disk(self.img)
        st = disk.status()
        self.assertEqual(st["state"], STATE_IDLE)
        self.assertEqual(disk.file_size, DATA_OFFSET + 14 * BLOCK_SIZE)
        for lbn in range(14):
            self.assertEqual(disk.read_lbn(lbn), block_content(lbn))
        disk.close()

    def test_kill_between_data_fsync_and_header_shows_pre_batch_layout(self):
        helper = self._helper()
        run = subprocess.run(
            [sys.executable, helper, self.img, "compact-hard-batch"],
            env=self._env(), capture_output=True, text=True, timeout=30)
        self.assertEqual(run.returncode, 42, run.stderr)
        self.assertIn("between data fsync and header commit", run.stdout)
        slots_before = [
            (lbn + 1) if lbn is not None else 0 for lbn in SEED_LAYOUT
        ]
        disk = Disk(self.img)
        st = disk.status()
        # 头未提交：重开只能看到整批搬移前的布局与游标 0。
        self.assertEqual(st["state"], STATE_COMPACTING)
        self.assertEqual(st["slots"], slots_before)
        self.assertEqual(st["cursor"], 0)
        self.assertEqual(st["moves"], 0)
        for lbn in range(14):
            self.assertEqual(disk.read_lbn(lbn), block_content(lbn))
        disk.close()

        # 继续整理仍能正常完成。
        out = subprocess.run(
            [sys.executable, helper, self.img, "resume"],
            env=self._env(), check=True, capture_output=True,
            text=True, timeout=30)
        self.assertIn("state=idle", out.stdout)
        disk = Disk(self.img)
        self.assertEqual(disk.file_size, DATA_OFFSET + 14 * BLOCK_SIZE)
        for lbn in range(14):
            self.assertEqual(disk.read_lbn(lbn), block_content(lbn))
        disk.close()

    def test_torn_header_copy_is_ignored(self):
        disk = Disk(self.img)
        disk.start_compaction()
        for _ in range(3):
            disk.compact_once()
        disk.close()
        with open(self.img, "r+b") as fh:
            active = self._active_copy(self.img)
            fh.seek(active * BLOCK_SIZE)
            fh.write(b"\xff" * BLOCK_SIZE)
            fh.flush()
            os.fsync(fh.fileno())
        disk = Disk(self.img)
        self.assertIn(disk.status()["state"],
                      (STATE_COMPACTING, STATE_IDLE))
        for lbn in range(14):
            self.assertEqual(disk.read_lbn(lbn), block_content(lbn))
        disk.close()

    @staticmethod
    def _active_copy(path: str) -> int:
        d = Disk(path)
        idx = d._active  # noqa: SLF001 - 测试需要内部指针
        d.close()
        return idx


class TestHeaderCodec(unittest.TestCase):
    def test_roundtrip(self):
        slots = [0, 1, 0, 5, 0]
        buf = _pack_header(7, STATE_COMPACTING, len(slots), 2, 3, 1, 11, 4,
                           slots)
        self.assertEqual(len(buf), BLOCK_SIZE)
        h = _unpack_header(buf)
        self.assertEqual(h["generation"], 7)
        self.assertEqual(h["state"], STATE_COMPACTING)
        self.assertEqual(h["slots"], slots)
        self.assertEqual(h["cursor"], 2)
        self.assertEqual(h["fsyncs"], 11)
        self.assertEqual(h["max_batch"], 4)

    def test_corruption_detected(self):
        buf = bytearray(_pack_header(1, STATE_IDLE, 3, 0, 0, 0, 0, 0,
                                     [0, 0, 0]))
        buf[100] ^= 0xFF
        with self.assertRaises(DiskError):
            _unpack_header(bytes(buf))


if __name__ == "__main__":
    unittest.main(verbosity=2)
