#!/usr/bin/env bash
# 故障注入演示：
#   1. 批中数据写报错（槽表不提交，重开只见批前布局）后续作；
#   2. 截短报错后数据仍可读，再续跑完成。
# 需要先启动网页：python -m app.web （或 docker compose up）
set -euo pipefail
BASE_URL="${1:-http://127.0.0.1:8000}"

wait_state() {
  local want="$1"
  for _ in $(seq 1 40); do
    state=$(curl -fsS "$BASE_URL/api/status" | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"])')
    [ "$state" = "$want" ] && { echo "$state"; return 0; }
    sleep 0.5
  done
  echo "$state"
  return 1
}

echo '== 重建出厂镜像 =='
curl -fsS -X POST "$BASE_URL/api/reseed" >/dev/null

echo
echo '== 注入“批中第 2 次数据写报错”并启动整理 =='
curl -fsS -X POST "$BASE_URL/api/fault/data-write?at=2" >/dev/null
curl -fsS -X POST "$BASE_URL/api/compact/start" >/dev/null
state=$(wait_state error)
echo "状态: $state（应为 error）"
curl -fsS "$BASE_URL/api/status" | python3 -c '
import json, sys
s = json.load(sys.stdin)
# 批处理未提交：进度不前进，批量统计为 0。
assert s["moves"] == 0 and s["max_batch_committed"] == 0 and s["cursor"] == 0, s
print("OK 槽表未提交：moves=%s cursor=%s，磁盘仍是整批前布局" % (s["moves"], s["cursor"]))
'
echo '== 错误态下读盘仍成功 =='
curl -fsS "$BASE_URL/api/blocks/0" | python3 -c 'import json,sys; r=json.load(sys.stdin); print("lbn", r["lbn"], "可读,", r["size"], "字节")'

echo
echo '== 续跑至完成 =='
curl -fsS -X POST "$BASE_URL/api/compact/start" >/dev/null
wait_state idle >/dev/null
curl -fsS "$BASE_URL/api/status" | python3 -c '
import json, sys
s = json.load(sys.stdin)
assert s["state"] == "idle"
assert s["file_size"] == 8192 + 14 * 4096
assert s["max_batch_committed"] == 4
assert s["fsyncs_per_batch_capacity"] == 2
print("OK 续作完成：file_size=%s 最大提交批量=%s 本轮fsync=%s errors=%s"
      % (s["file_size"], s["max_batch_committed"], s["fsyncs"], s["errors"]))
'

echo
echo '== 再演示截短报错（重建镜像后） =='
curl -fsS -X POST "$BASE_URL/api/reseed" >/dev/null
curl -fsS -X POST "$BASE_URL/api/fault/truncate" >/dev/null
curl -fsS -X POST "$BASE_URL/api/compact/start" >/dev/null
wait_state error >/dev/null
curl -fsS -X POST "$BASE_URL/api/compact/start" >/dev/null
wait_state idle >/dev/null
curl -fsS "$BASE_URL/api/status" | python3 -c '
import json, sys
s = json.load(sys.stdin)
assert s["state"] == "idle" and s["file_size"] == 8192 + 14 * 4096
print("OK 截短故障后续作完成，file_size=%s" % s["file_size"])
'
