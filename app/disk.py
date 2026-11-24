"""单卷虚拟磁盘：带空槽的本地镜像 + 崩溃安全的批量原地整理。

镜像布局
========
偏移 0            : 主头副本   （4 KiB）
偏移 4096         : 备头副本   （4 KiB）
偏移 8192 + i*4096: 物理槽 i  （4 KiB，共 slot_count 个）

头内槽表 slots[] 每项为 int32：0 表示空槽，否则是 (逻辑块号 + 1)。

崩溃安全（按批提交）
====================
- 每个头带 16 位结构版本、单调递增世代号和整头 CRC32。
- 提交时只写“当前非活动”的那个副本，再把活动指针切过去。
  因此任何时刻至少有一个完整有效的旧头；重启时取世代号最大的有效头。
- 整理以“批”为最小提交单位，每批最多搬运 MAX_BATCH=4 个块：
    1. 逐块把数据写入各自的目标空槽（经一个复用的 4 KiB 暂存缓冲），
       全部写完后对镜像执行第 1 次 fsync；
    2. 把整批的新槽表、游标、搬移计数、批量统计写进非活动头副本并
       fsync（第 2 次），切换活动副本。
  槽表提交前所有读仍命中旧位置；提交后每个目标槽已有完整数据。
  因此一次批处理中途退出（含 SIGKILL）后重开，只能看到整批搬移前
  或整批搬移后的布局，永远看不到半截槽表。
- 需要借空槽周转时，“占槽块 → 空枢轴槽”单独构成一个 1 块批（同样
  最多 2 次 fsync），随后目标块即可在下一批直接进入最终空槽。
- 整理游标 cursor 随槽表一起持久化，且只在批提交时推进，进度只反映
  已提交的搬移；进程中途退出后重启可从游标继续。

fsync 计数
==========
头里持久化本轮整理（进入 compacting 之后到回到 idle 之间）实际执行的
fsync 次数 fsyncs 与已提交的最大批量 max_batch；中途出错/退出后续作
不重置，重新对一个 idle 卷发起新一轮整理时清零。进入 compacting 的
那次状态提交发生在计数置位之前，不计入本轮。

镜像外临时数据
==============
整理只使用一个复用的暂存缓冲，任何时刻最多持有一个 4 KiB 块
（Disk.max_temp_seen 可观测），不存在第二份临时块。
"""

from __future__ import annotations

import contextlib
import io
import os
import struct
import threading
import zlib

BLOCK_SIZE = 4096
HEADER_BLOCKS = 2
HEADER_SIZE = HEADER_BLOCKS * BLOCK_SIZE
DATA_OFFSET = HEADER_SIZE

# 每批最多搬 4 块；连同进度更新，一批对镜像最多执行两次 fsync。
MAX_BATCH = 4
# 每批固定两步落盘：数据一次、盘头一次。
FSYNCS_PER_MOVE_BATCH = 2

MAGIC = b"VDEF"
HDR_VERSION = 2
# 32 个槽位，留足空槽；需要调整时重新生成镜像即可。
DEFAULT_SLOT_COUNT = 32

STATE_IDLE = "idle"
STATE_COMPACTING = "compacting"
STATE_ERROR = "error"
STATES = (STATE_IDLE, STATE_COMPACTING, STATE_ERROR)

# 头固定区：
# magic(4) version(H) generation(Q) state(16) slot_count(H) cursor(H)
# moves(I) errors(I) fsyncs(I) max_batch(I) crc(I)，共 56 字节；
# 其余留给槽表。
HDR_FIXED = struct.Struct("<4sHQ16sHHIIIII")
HDR_FIXED_SIZE = HDR_FIXED.size  # 56
HDR_PAYLOAD_CAP = BLOCK_SIZE - 4  # 末尾 4 字节放 crc32
SLOTS_MAX = (HDR_PAYLOAD_CAP - HDR_FIXED_SIZE) // 4
STATE_BYTES = 16


class DiskError(RuntimeError):
    """磁盘逻辑错误（损坏、非法状态等），与搬移/截短的注入错误区分开。"""


class RWLock:
    """多读单写锁：读之间不互斥，写独占。

    写者在条件变量上与后来的读者一同竞争；本项目只有单个整理工作线程，
    且每次持锁时间极短（磁盘 I/O 在锁外），不会出现写者饥饿。
    """

    def __init__(self) -> None:
        self._cond = threading.Condition(threading.Lock())
        self._readers = 0
        self._writers = 0

    @contextlib.contextmanager
    def read_lock(self):
        with self._cond:
            while self._writers:
                self._cond.wait()
            self._readers += 1
        try:
            yield
        finally:
            with self._cond:
                self._readers -= 1
                if self._readers == 0:
                    self._cond.notify_all()

    @contextlib.contextmanager
    def write_lock(self):
        with self._cond:
            while self._readers or self._writers:
                self._cond.wait()
            self._writers += 1
        try:
            yield
        finally:
            with self._cond:
                self._writers -= 1
                self._cond.notify_all()


def _pack_header(generation: int, state: str, slot_count: int,
                 cursor: int, moves: int, errors: int,
                 fsyncs: int, max_batch: int, slots: list[int]) -> bytes:
    if state not in STATES:
        raise ValueError(f"unknown state {state!r}")
    if len(slots) != slot_count or slot_count > SLOTS_MAX:
        raise DiskError("bad slot table length")
    if not 0 <= cursor <= slot_count:
        raise DiskError("cursor out of range")
    payload = HDR_FIXED.pack(
        MAGIC, HDR_VERSION, generation,
        state.encode("ascii").ljust(STATE_BYTES, b"\x00"),
        slot_count, cursor, moves, errors, fsyncs, max_batch,
        0,  # crc 占位
    )
    payload += struct.pack(f"<{slot_count}i", *slots)
    payload += b"\x00" * (HDR_PAYLOAD_CAP - len(payload))
    crc = zlib.crc32(payload) & 0xFFFFFFFF
    return payload + struct.pack("<I", crc)


def _unpack_header(buf: bytes) -> dict:
    if len(buf) != BLOCK_SIZE:
        raise DiskError("header block size mismatch")
    payload, crc_bytes = buf[:HDR_PAYLOAD_CAP], buf[HDR_PAYLOAD_CAP:]
    (crc_stored,) = struct.unpack("<I", crc_bytes)
    if (zlib.crc32(payload) & 0xFFFFFFFF) != crc_stored:
        raise DiskError("header crc mismatch")
    (magic, version, generation, state_raw, slot_count, cursor,
     moves, errors, fsyncs, max_batch, _crc_zero) = HDR_FIXED.unpack(
         payload[:HDR_FIXED_SIZE])
    if magic != MAGIC or version != HDR_VERSION:
        raise DiskError("bad magic/version")
    if slot_count > SLOTS_MAX:
        raise DiskError("slot_count too large")
    slots = list(struct.unpack(f"<{slot_count}i",
                               payload[HDR_FIXED_SIZE:
                                       HDR_FIXED_SIZE + slot_count * 4]))
    state = state_raw.rstrip(b"\x00").decode("ascii")
    if state not in STATES or not 0 <= cursor <= slot_count:
        raise DiskError("bad state/cursor in header")
    return {
        "generation": generation,
        "state": state,
        "slot_count": slot_count,
        "cursor": cursor,
        "moves": moves,
        "errors": errors,
        "fsyncs": fsyncs,
        "max_batch": max_batch,
        "slots": slots,
    }


def lbn_to_slot(slots: list[int]) -> dict[int, int]:
    """逻辑块号 -> 物理槽号；返回重复映射会报错。"""
    mapping: dict[int, int] = {}
    for phys, entry in enumerate(slots):
        if entry:
            lbn = entry - 1
            if lbn in mapping:
                raise DiskError(f"duplicate mapping for lbn {lbn}")
            mapping[lbn] = phys
    return mapping


class Disk:
    """打开/创建一个镜像文件并提供读块、批量整理、状态查询。"""

    def __init__(self, path: str) -> None:
        self.path = path
        self._fd = os.open(path, os.O_RDWR)
        self.lock = RWLock()
        self._active = -1
        self._staging: io.BytesIO | None = None
        # 一次性故障注入：下一次对应操作抛 RuntimeError，不改变持久状态。
        # inject_move_error 在批处理做任何 I/O 之前触发（整批尚未开始）。
        self.inject_move_error = False
        self.inject_truncate_error = False
        # 数据写故障：>0 表示本批第 N 次数据 pwrite 时抛错（1 基），
        # 用于模拟“批处理写到一半磁盘出错”，槽表在该批绝不提交。
        self.inject_data_write_error_at = 0
        self.move_delay = 0.0
        self.max_temp_seen = 0
        self._load_header()
        # 重开时若正处在一轮整理中（compacting/error），后续 fsync 继续
        # 计入本轮；start_compaction 从 idle 开新一轮时会清零并重新置位。
        self._counting_fsync = self.state in (STATE_COMPACTING, STATE_ERROR)

    # ---------- 打开 / 创建 ----------

    @classmethod
    def create(cls, path: str, slot_count: int = DEFAULT_SLOT_COUNT) -> "Disk":
        if os.path.exists(path):
            raise FileExistsError(path)
        slots = [0] * slot_count
        hdr = _pack_header(1, STATE_IDLE, slot_count, 0, 0, 0, 0, 0, slots)
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            os.pwrite(fd, hdr, 0)
            os.pwrite(fd, hdr, BLOCK_SIZE)
            # 预分配槽位（稀疏/零填充均可，ftruncate 不实际写数据）。
            os.ftruncate(fd, DATA_OFFSET + slot_count * BLOCK_SIZE)
            os.fsync(fd)
        finally:
            os.close(fd)
        return cls(path)

    def _load_header(self) -> None:
        candidates: list[tuple[int, dict]] = []
        for idx in range(HEADER_BLOCKS):
            raw = os.pread(self._fd, BLOCK_SIZE, idx * BLOCK_SIZE)
            try:
                candidates.append((idx, _unpack_header(raw)))
            except DiskError:
                continue
        if not candidates:
            raise DiskError("no valid header copy; image unusable")
        # 取世代号最大的有效副本（世代号单调递增，不会打平）。
        self._active, best = max(candidates, key=lambda item: item[1][
            "generation"])
        self._gen = best["generation"]
        self.state = best["state"]
        self.slot_count = best["slot_count"]
        self.cursor = best["cursor"]
        self.moves = best["moves"]
        self.errors = best["errors"]
        self.fsyncs = best["fsyncs"]
        self.max_batch = best["max_batch"]
        self.slots = list(best["slots"])

    def close(self) -> None:
        with self.lock.write_lock():
            if self._staging is not None:
                self._staging.close()
                self._staging = None
            os.close(self._fd)
            self._fd = -1

    # ---------- 持久化 ----------

    def _fsync(self) -> None:
        """fsync 镜像并计数（仅一轮整理进行中才计入本轮统计）。"""
        os.fsync(self._fd)
        if self._counting_fsync:
            self.fsyncs += 1

    def _commit(self) -> None:
        """把当前内存状态写进非活动头副本，fsync 后切换活动指针。"""
        if self._fd < 0:
            raise DiskError("disk closed")
        buf = _pack_header(self._gen + 1, self.state, self.slot_count,
                           self.cursor, self.moves, self.errors,
                           self.fsyncs, self.max_batch, self.slots)
        target = 1 - self._active
        os.pwrite(self._fd, buf, target * BLOCK_SIZE)
        # _counting_fsync 在进入 compacting 的提交之后才置位，因此该次
        # 启动提交不计入本轮；其后所有批/截短/错误提交都会计数。
        self._fsync()
        self._active = target
        self._gen += 1

    # ---------- 槽位读写 ----------

    def _slot_offset(self, phys: int) -> int:
        if not 0 <= phys < self.slot_count:
            raise DiskError("physical slot out of range")
        return DATA_OFFSET + phys * BLOCK_SIZE

    def _read_slot(self, phys: int) -> bytes:
        data = os.pread(self._fd, BLOCK_SIZE, self._slot_offset(phys))
        if len(data) != BLOCK_SIZE:
            raise DiskError(f"short read at slot {phys}")
        return data

    def _write_slot_now(self, phys: int, data: bytes) -> None:
        """只写数据槽，不 fsync；fsync 由批处理统一做（每批一次）。"""
        if len(data) != BLOCK_SIZE:
            raise DiskError("block must be exactly 4 KiB")
        os.pwrite(self._fd, data, self._slot_offset(phys))

    def _write_slot(self, phys: int, data: bytes) -> None:
        """建镜像用：写一个数据槽并立即 fsync（仅 idle 态 seed 路径）。"""
        self._write_slot_now(phys, data)
        os.fsync(self._fd)

    # ---------- 对外：读逻辑块 ----------

    def read_lbn(self, lbn: int) -> bytes:
        """按逻辑块号读 4 KiB；整理过程中始终返回完整原数据。"""
        if lbn < 0:
            raise DiskError("lbn must be non-negative")
        with self.lock.read_lock():
            entry = lbn + 1
            if entry not in self.slots:
                raise KeyError(lbn)
            phys = self.slots.index(entry)
        # 数据位置在提交新头之前不会被覆盖，pread 放到锁外不影响正确性，
        # 也避免整理的磁盘 I/O 长时间挡住读请求（读块不等整批写完）。
        return self._read_slot(phys)

    def has_lbn(self, lbn: int) -> bool:
        with self.lock.read_lock():
            return (lbn + 1) in self.slots

    def used_lbns(self) -> list[int]:
        with self.lock.read_lock():
            return sorted(e - 1 for e in self.slots if e)

    # ---------- 建镜像用：写块（仅 idle 态，供种子脚本调用） ----------

    def seed_write(self, lbn: int, data: bytes, phys: int) -> None:
        with self.lock.write_lock():
            if self.state != STATE_IDLE:
                raise DiskError("seed write only allowed in idle state")
            if lbn < 0 or (lbn + 1) in self.slots:
                raise DiskError(f"lbn {lbn} already mapped")
            if not 0 <= phys < self.slot_count or self.slots[phys] != 0:
                raise DiskError(f"slot {phys} not free")
            self._write_slot(phys, data)
            self.slots[phys] = lbn + 1
            self._commit()

    # ---------- 整理 ----------

    @property
    def file_size(self) -> int:
        return os.fstat(self._fd).st_size

    def expected_compact_size(self) -> int:
        return DATA_OFFSET + len(self.used_lbns()) * BLOCK_SIZE

    def start_compaction(self) -> bool:
        """进入整理态并持久化；已在整理态返回 False。

        从 idle 发起的是新一轮整理，游标与本轮统计（moves/fsyncs/
        max_batch）清零；从 error 续作时保留全部进度与统计。
        """
        with self.lock.write_lock():
            if self.state == STATE_COMPACTING:
                return False
            if self.state == STATE_ERROR:
                # 错误态保留游标与统计，允许值班员重试续作。
                self.state = STATE_COMPACTING
                self._commit()
                self._counting_fsync = True
                return True
            self.state = STATE_COMPACTING
            self.cursor = 0
            self.moves = 0
            self.fsyncs = 0
            self.max_batch = 0
            self._commit()
            self._counting_fsync = True
            return True

    def _get_staging(self) -> io.BytesIO:
        if self._staging is None:
            # 内存暂存即“镜像外临时块”，生命周期内最多 4 KiB。
            self._staging = io.BytesIO()
        return self._staging

    def _temp_hold(self, data: bytes) -> None:
        st = self._get_staging()
        st.seek(0)
        st.truncate(0)
        st.write(data)
        st.seek(0)
        self.max_temp_seen = max(self.max_temp_seen, st.getbuffer().nbytes)

    def _temp_release(self) -> bytes:
        st = self._get_staging()
        data = st.getvalue()
        st.seek(0)
        st.truncate(0)
        return data

    def _find_free_from(self, start: int) -> int:
        for phys in range(start, self.slot_count):
            if self.slots[phys] == 0:
                return phys
        return -1

    def _plan_batch(self) -> tuple[list[tuple[int, int]], int, bool]:
        """在当前槽表上规划下一批。

        返回 (moves, new_cursor, pivot)：
        - moves: [(src_phys, dst_phys), ...]，长度 0..MAX_BATCH；
                 非空时所有 dst 在当前槽表中都是空槽，src/dst 两两不重叠；
        - new_cursor: 本批提交后游标应到达的位置；
        - pivot: True 表示这是“占槽块 → 空枢轴槽”的周转批（1 块），
                 游标不前进，下一批再把目标块搬进最终空槽。

        不变量：进入游标 i 时，物理槽 0..i-1 已依次压紧 used[0..i-1]，
        其余 U-i 个在用块分布在槽 i..S-1 中。因 S>U，该区间恒有空槽，
        枢轴永远可得。
        """
        used = sorted(e - 1 for e in self.slots if e)
        mapping = lbn_to_slot(self.slots)
        i = self.cursor
        # 先越过已经落在正确最终槽位的块（压紧镜像重新整理时整段跳过）。
        while i < len(used) and mapping[used[i]] == i:
            i += 1
        if i >= len(used):
            return [], i, False
        if self.slots[i] != 0:
            # 最终槽 i 被别的在用块占着：先借一个 ≥ i 的空槽周转。
            pivot_slot = self._find_free_from(i)
            if pivot_slot < 0:
                raise DiskError("no free slot available for pivoting")
            return [(i, pivot_slot)], self.cursor, True
        # 槽 i 为空：目标块可直接进入最终槽。把后续“可直接进入各自当前
        # 为空的最终槽”的块合批，最多 MAX_BATCH 块；一旦最终槽被占用
        # 就停，下批走枢轴周转。
        moves: list[tuple[int, int]] = []
        while i < len(used) and len(moves) < MAX_BATCH:
            target_phys = mapping[used[i]]
            if target_phys == i:
                i += 1  # 已就位（合批扫描中较少见，稳妥起见仍跳过）
                continue
            if self.slots[i] != 0:
                break
            moves.append((target_phys, i))
            i += 1
        return moves, i, False

    def _execute_batch(self, moves: list[tuple[int, int]],
                       new_cursor: int) -> None:
        """执行一批搬移并原子提交；任何写盘错误都在提交前抛出。

        一批对镜像恰好两次 fsync（数据一次、盘头一次），进度随盘头一起
        提交，因此本批要么整体可见、要么完全不可见。
        """
        if not 1 <= len(moves) <= MAX_BATCH:
            raise DiskError("batch size out of range")
        if self.inject_move_error:
            # 在任何 I/O 之前失败：盘上状态完全不变。
            self.inject_move_error = False
            raise RuntimeError("injected move failure")
        # 快照校验：源槽在用、目标槽为空、src/dst 两两不重叠。
        with self.lock.read_lock():
            entries: list[int] = []
            seen: set[int] = set()
            for src, dst in moves:
                if self.slots[src] == 0:
                    raise DiskError(f"source slot {src} is empty")
                if self.slots[dst] != 0:
                    raise DiskError(f"dest slot {dst} not free")
                if src in seen or dst in seen:
                    raise DiskError("batch slots overlap")
                seen.add(src)
                seen.add(dst)
                entries.append(self.slots[src])

        # 1) 逐块读源槽 -> 4 KiB 暂存 -> 写目标槽（暂存每块复用，
        #    镜像外任意时刻最多 4 KiB）。读块不需要等整批写完。
        write_index = 0
        for src, dst in moves:
            if self.move_delay:
                threading.Event().wait(self.move_delay)
            data = self._read_slot(src)
            self._temp_hold(data)
            write_index += 1
            if (self.inject_data_write_error_at
                    and write_index == self.inject_data_write_error_at):
                self.inject_data_write_error_at = 0
                raise RuntimeError("injected data write failure")
            self._write_slot_now(dst, data)
            self._temp_release()

        # 2) 整批数据统一落盘：本批第 1 次 fsync。
        self._fsync()

        # 3) 提交整批新槽表与进度：本批第 2 次 fsync（在 _commit 内）。
        with self.lock.write_lock():
            # 单工作线程模型：期间没有别的写者，再确认一次快照未漂移。
            for (src, dst), entry in zip(moves, entries):
                if self.slots[src] != entry or self.slots[dst] != 0:
                    raise DiskError("slot table changed unexpectedly")
            for (src, dst), entry in zip(moves, entries):
                self.slots[src] = 0
                self.slots[dst] = entry
            self.moves += len(moves)
            self.max_batch = max(self.max_batch, len(moves))
            self.cursor = new_cursor
            self._commit()

    def compact_once(self) -> bool:
        """推进整理一批；返回 True 表示全部完成（已截短并回到 idle）。

        批是最小崩溃安全单位：函数返回前若进程被杀，重开只能看到本批
        之前或之后的完整布局，游标只在批提交后推进。
        """
        with self.lock.write_lock():
            if self.state != STATE_COMPACTING:
                if self.state == STATE_IDLE:
                    return True
                raise DiskError(f"cannot compact in state {self.state}")
            used_count = len([e for e in self.slots if e])
            slot_count = self.slot_count
            cursor = self.cursor

        if cursor < used_count:
            moves, new_cursor, _pivot = self._plan_batch()
            if not moves:
                # 游标之后的块都已在正确槽位（例如对压紧镜像重新整理）：
                # 只推进游标，不产生搬移，也不额外 fsync；下一轮截短。
                with self.lock.write_lock():
                    self.cursor = new_cursor
                return False
            self._execute_batch(moves, new_cursor)
            return False

        # 所有在用块已压紧到前 used_count 个槽，截短尾部。
        target_size = DATA_OFFSET + used_count * BLOCK_SIZE
        if self.inject_truncate_error:
            self.inject_truncate_error = False
            raise RuntimeError("injected truncate failure")
        with self.lock.write_lock():
            # 截短前再校验一次，防止截掉在用数据。
            for phys in range(used_count, slot_count):
                if self.slots[phys] != 0:
                    raise DiskError("refuse to truncate allocated slot")
            os.ftruncate(self._fd, target_size)
            self._fsync()
            # 游标停在在用块数：前 cursor 个槽均已压紧，idle 态再触发时
            # compact_once 的首分支会立即返回完成。
            self.cursor = used_count
            self.state = STATE_IDLE
            self._commit()
            # 本轮整理结束：此后的 fsync 不再计入本轮统计。
            self._counting_fsync = False
        return True

    def record_error(self) -> None:
        with self.lock.write_lock():
            self.errors += 1
            self.state = STATE_ERROR
            self._commit()

    # ---------- 状态快照 ----------

    def status(self) -> dict:
        with self.lock.read_lock():
            used = sorted(e - 1 for e in self.slots if e)
            slots = list(self.slots)
            state = self.state
            cursor = self.cursor
            moves = self.moves
            errors = self.errors
            fsyncs = self.fsyncs
            max_batch = self.max_batch
            gen = self._gen
        return {
            "path": self.path,
            "state": state,
            "slot_count": self.slot_count,
            "used_count": len(used),
            "used_lbns": used,
            "slots": slots,  # 0=空槽，否则为 lbn+1
            "cursor": cursor,
            "moves": moves,
            "errors": errors,
            "fsyncs": fsyncs,
            "max_batch_committed": max_batch,
            "max_batch_capacity": MAX_BATCH,
            "fsyncs_per_batch_capacity": FSYNCS_PER_MOVE_BATCH,
            "generation": gen,
            "file_size": self.file_size,
            "block_size": BLOCK_SIZE,
            "expected_compact_size":
                DATA_OFFSET + len(used) * BLOCK_SIZE,
            "max_temp_bytes": self.max_temp_seen,
        }


def run_compaction_worker(disk: Disk, stop_event: threading.Event | None =
                          None) -> None:
    """后台工作线程：循环按批推进整理，出错置 error 态并保留游标。"""
    while True:
        if stop_event is not None and stop_event.is_set():
            return
        try:
            done = disk.compact_once()
        except Exception:
            disk.record_error()
            raise
        if done:
            return
