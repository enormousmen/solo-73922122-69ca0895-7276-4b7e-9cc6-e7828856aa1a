"""生成内置演示镜像 data/disk.img（带空槽的单卷）。

物理布局（None 表示空槽，数字表示该槽存放的逻辑块号）::

    [空, 空, 空, 空, 0, 1, 2, 3, 4, 空, 5, 6, 7, 8, 9, 10, 11, 12, 13, 空]

共 20 个槽、14 个在用块（lbn 0..13）、6 个空槽。头部连续 4 个最终空槽，
使整理一上来就能合出 4 块一批的大批量；随后两批各 4 块、最后一批 2 块，
批次序列为 4 + 4 + 4 + 2（共 14 次搬移）。

整理完成后文件长度应为 8192 + 14*4096 = 65536 字节，
lbn 0..13 依次落在物理槽 0..13，各块内容字节不变。
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.disk import BLOCK_SIZE, Disk  # noqa: E402

# 物理槽 -> 逻辑块号；None 表示空槽。
SEED_LAYOUT = [
    None, None, None, None, 0, 1, 2, 3, 4, None,
    5, 6, 7, 8, 9, 10, 11, 12, 13, None,
]


def block_content(lbn: int) -> bytes:
    """确定性内容：头部可读标签 + 可校验的确定性填充，恰好 4 KiB。"""
    head = f"lbn={lbn:<6d} seed-block content ".encode("ascii")
    buf = bytearray(head)
    x = (lbn * 2654435761) & 0xFFFFFFFF
    while len(buf) < BLOCK_SIZE:
        x = (1103515245 * x + 12345) & 0x7FFFFFFF
        buf.append(x & 0xFF)
    return bytes(buf[:BLOCK_SIZE])


def build(path: str, layout: list[int | None] | None = None) -> None:
    layout = SEED_LAYOUT if layout is None else layout
    if os.path.exists(path):
        os.remove(path)
    disk = Disk.create(path, slot_count=len(layout))
    try:
        for phys, lbn in enumerate(layout):
            if lbn is not None:
                disk.seed_write(lbn, block_content(lbn), phys)
        status = disk.status()
        print(f"created {path}")
        print(f"  slots={status['slot_count']} used={status['used_count']} "
              f"file_size={status['file_size']}")
    finally:
        disk.close()


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "disk.img")
    os.makedirs(os.path.dirname(target) or ".", exist_ok=True)
    build(target)
