"""命令行工具：便于在容器/宿主机上做可复现的中断续作演示。

子命令：
  seed <img>                 重建内置演示镜像
  status <img>               打印卷状态 JSON
  verify <img>               校验所有在用逻辑块可读、映射自洽、内容与种子一致
  compact-once <img>         推进整理一批（打印是否完成）
  hard-exit <img>            进入整理态、提交一批后 os._exit 硬退出（模拟 SIGKILL）
  hard-exit-batch <img>      数据已 fsync、盘头提交前 os._exit 硬退出，
                             验证重开只能看到整批搬移前的布局
  resume <img>               续跑整理直到完成
"""

from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.disk import DATA_OFFSET, STATE_IDLE, Disk, lbn_to_slot  # noqa: E402
from app.seed import block_content, build as build_seed  # noqa: E402


def cmd_seed(img: str) -> int:
    build_seed(img)
    return 0


def cmd_status(img: str) -> int:
    disk = Disk(img)
    try:
        print(json.dumps(disk.status(), ensure_ascii=False, indent=2))
    finally:
        disk.close()
    return 0


def cmd_verify(img: str) -> int:
    disk = Disk(img)
    try:
        st = disk.status()
        mapping = lbn_to_slot(st["slots"])
        problems: list[str] = []
        for lbn in st["used_lbns"]:
            data = disk.read_lbn(lbn)
            if len(data) != 4096:
                problems.append(f"lbn {lbn} 长度 {len(data)} != 4096")
                continue
            # 演示镜像的块内容由 app.seed.block_content 确定性生成。
            if data != block_content(lbn):
                problems.append(f"lbn {lbn} 内容与种子不一致")
        # 长度不变量：下界=头+全部在用块（压紧后的最小长度），
        # 上界=头+声明槽位数（未整理的初始长度）；截短只会落在区间内。
        lower = DATA_OFFSET + st["used_count"] * 4096
        upper = DATA_OFFSET + st["slot_count"] * 4096
        if not lower <= disk.file_size <= upper:
            problems.append(
                "文件长度 %s 超出合法区间 [%s, %s]"
                % (disk.file_size, lower, upper))
        # 任何已映射的物理槽都必须落在文件边界内。
        max_phys = max(mapping.values()) if mapping else -1
        if max_phys >= 0 and DATA_OFFSET + (max_phys + 1) * 4096 \
                > disk.file_size:
            problems.append(
                "物理槽 %s 的映射超出文件长度 %s" % (max_phys, disk.file_size))
        print(json.dumps({
            "state": st["state"],
            "file_size": disk.file_size,
            "used_lbns": st["used_lbns"],
            "mapping_lbn_to_slot": mapping,
            "problems": problems,
            "ok": not problems,
        }, ensure_ascii=False, indent=2))
        return 0 if not problems else 1
    finally:
        disk.close()


def cmd_compact_once(img: str) -> int:
    disk = Disk(img)
    try:
        if disk.state == STATE_IDLE:
            print("already idle")
            return 0
        cursor_before = disk.cursor
        done = disk.compact_once()
        print(f"done={done} cursor {cursor_before}->{disk.cursor} "
              f"moves={disk.moves} max_batch={disk.max_batch} "
              f"fsyncs={disk.fsyncs} state={disk.state}")
        return 0
    finally:
        disk.close()


def cmd_hard_exit(img: str) -> int:
    disk = Disk(img)
    disk.start_compaction()
    print("COMPACT_STARTED", flush=True)
    disk.compact_once()  # 完整提交一批后硬退出
    print(f"hard-exit after one committed batch: cursor={disk.cursor} "
          f"moves={disk.moves} max_batch={disk.max_batch} "
          f"fsyncs={disk.fsyncs}", flush=True)
    # 立即结束解释器：不执行 finally、不关闭 fd、不刷新用户态缓冲，
    # 与 SIGKILL 后的恢复条件一致（只能靠已 fsync 的盘上状态）。
    os._exit(42)


def cmd_hard_exit_batch(img: str) -> int:
    """在“数据已 fsync、盘头尚未提交”的窗口硬退出。

    磁盘上目标槽已写入整批数据，但活动头仍描述批前槽表；重开后必须只
    看到整批搬移前的布局，且可以原样继续整理。
    """
    disk = Disk(img)
    disk.start_compaction()
    print("COMPACT_STARTED", flush=True)
    moves, new_cursor, _ = disk._plan_batch()  # noqa: SLF001 - CLI 演示钩子
    print(f"planned batch of {len(moves)} move(s): {moves}", flush=True)

    real_commit = disk._commit  # noqa: SLF001

    def kill_instead_of_commit() -> None:
        print(f"hard-exit between data fsync and header commit: "
              f"cursor stays {disk.cursor} (new would be {new_cursor})",
              flush=True)
        os._exit(42)

    disk._commit = kill_instead_of_commit  # type: ignore[method-assign]
    try:
        disk._execute_batch(moves, new_cursor)  # noqa: SLF001
    finally:
        disk._commit = real_commit  # type: ignore[method-assign]
    raise SystemExit("unreachable: _execute_batch should have hard-exited")


def cmd_resume(img: str) -> int:
    disk = Disk(img)
    try:
        # compacting/error 态是续作（保留统计）；idle 态则发起新一轮整理。
        disk.start_compaction()
        batches = 0
        while True:
            done = disk.compact_once()
            batches += 1
            if done:
                break
        st = disk.status()
        print(f"resumed and finished in {batches} batches; "
              f"moves={st['moves']} max_batch={st['max_batch_committed']} "
              f"fsyncs={st['fsyncs']} file_size={st['file_size']} "
              f"state={st['state']}")
        return 0
    finally:
        disk.close()


def main(argv: list[str]) -> int:
    if len(argv) < 3:
        print(__doc__)
        return 2
    command, img = argv[1], argv[2]
    handlers = {
        "seed": cmd_seed,
        "status": cmd_status,
        "verify": cmd_verify,
        "compact-once": cmd_compact_once,
        "hard-exit": cmd_hard_exit,
        "hard-exit-batch": cmd_hard_exit_batch,
        "resume": cmd_resume,
    }
    if command not in handlers:
        print(f"unknown command: {command}\n{__doc__}")
        return 2
    return handlers[command](img)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
