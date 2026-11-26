#!/bin/bash
set -euo pipefail

cd /home/stevensxy/cfip-tools/linux_py

# 测速阶段必须直连（防止代理干扰 --resolve 测速）
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}" )" && pwd)"

# 阶段1: cfip 测速（不推送，合并后再统一推）
/usr/bin/python3 update.py \
  --NO \
  --top 10 \
  --min-speed 12 \
  --max-ports-per-ip 2 \
  --regions "JP,SG,KR,TW" \
  --no-github-sync

# 阶段2: 历史池补位合并（TCP 预检 + 实时测速补位；慢节点由每日复测纠正）
LOG_DIR="${CRON_OUTPUT_DIR:-/home/stevensxy/.hermes/cron/output/a1497428ad91}"
# 按天分文件：旧版累积单文件且行内无时间戳，任何"最近 N 天"的分析都无从下手（2026-09-12 修复）
MERGE_LOG="$LOG_DIR/merge-history-$(TZ='Asia/Shanghai' date +%F).log"
set +e  # merge 失败不应阻断后续 push(已有当次 best 可推)
/usr/bin/python3 "$SCRIPT_DIR/merge-history.py" \
  --best /home/stevensxy/cfip-tools/linux_py/best_ips.txt \
  --pool "$SCRIPT_DIR/history_pool.json" 2>&1 \
  | awk '{ print strftime("%Y-%m-%d %H:%M:%S"), $0; fflush() }' \
  | tee -a "$MERGE_LOG"
MERGE_STATUS=${PIPESTATUS[0]}
set -e
if [ "$MERGE_STATUS" -ne 0 ]; then
  echo "ERROR: merge-history.py failed (exit $MERGE_STATUS), continuing with raw best_ips.txt" >&2
fi

# ── 阶段 2.5: 测速环境自检 + 空结果保护 ──
# 2026-11-08 实测: 12:06 那轮 50 候选只出 2 个 fast(6:01 同批节点出 16 个), 榜单被塌成 2 条
# 覆盖到 GitHub。根因是测速当时带宽被占, 不是节点变差。原阈值 5 在「只出 2 条」时不触发。
BEST_COUNT=$(wc -l < /home/stevensxy/cfip-tools/linux_py/best_ips.txt | tr -d ' ')
MIN_BEST_FOR_PUSH=8

# 基线带宽自检: 直连拉 8MB, 低于 MIN_BASELINE_MBPS 说明家宽/上游正被占用, 本轮测速不可信
BASELINE_URL="https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/24.04/ubuntu-24.04.3-live-server-amd64.iso"
BASELINE_BPS=$(curl -sL -o /dev/null -r 0-8000000 -w '%{speed_download}' \
  --noproxy '*' --max-time 25 "$BASELINE_URL" 2>/dev/null || echo 0)
BASELINE_MBPS=$(awk -v b="${BASELINE_BPS:-0}" 'BEGIN{printf "%.1f", b*8/1000000}')
MIN_BASELINE_MBPS=40

if awk -v v="$BASELINE_MBPS" -v t="$MIN_BASELINE_MBPS" 'BEGIN{exit !(v < t)}'; then
  echo "⚠️ 测速环境自检: 基线带宽 ${BASELINE_MBPS}Mbps < ${MIN_BASELINE_MBPS}Mbps, 本机带宽被占用, 测速结果不可信 → 跳过本次 push"
  exit 0
fi
echo "✅ 测速环境自检: 基线带宽 ${BASELINE_MBPS}Mbps ≥ ${MIN_BASELINE_MBPS}Mbps, 结果可用"

if [ "$BEST_COUNT" -lt "$MIN_BEST_FOR_PUSH" ]; then
  echo "⚠️ 空结果保护: best_ips.txt 仅 $BEST_COUNT 行 (< $MIN_BEST_FOR_PUSH), 疑似测速环境异常, 跳过本次 push 以保护 GitHub 现有榜单"
  # merge-history 已在本轮完成池维护(容错驱逐/补位), 这里只挡推送
  exit 0
fi

# 阶段3: 统一推送到 GitHub（best + full + README）
WORK_DIR="/home/stevensxy/cfip-tools/linux_py"
SYNC_DIR="$WORK_DIR/.github-sync"
REPO="https://github.com/StevenSXY/cfbest.git"
BRANCH="results"
GIT_PROXY_ARGS=(-c "http.proxy=http://127.0.0.1:7897" -c "http.sslVerify=false")

# clone（不存在则建；存在则 fetch+reset）
if [ ! -d "$SYNC_DIR/.git" ]; then
  git "${GIT_PROXY_ARGS[@]}" clone --branch "$BRANCH" --single-branch "$REPO" "$SYNC_DIR"
else
  cd "$SYNC_DIR"
  git "${GIT_PROXY_ARGS[@]}" fetch origin "$BRANCH"
  git reset --hard
  git checkout -B "$BRANCH" "origin/$BRANCH"
  cd "$WORK_DIR"
fi

# 复制结果 + 生成 README
cp "$WORK_DIR/best_ips.txt" "$WORK_DIR/full_ips.txt" "$SYNC_DIR/"
{
  echo "# CF Best IP Results"
  echo ""
  echo "**更新频率**: 每 1 小时"
  echo "**区域**: JP,SG,KR,TW,HK"
  echo "**测试位置**: 中国电信"
  printf "**本次更新**: %s\n" "$(TZ='Asia/Shanghai' date '+%Y-%m-%d %H:%M:%S')"
  echo ""
  echo '```'
  cat "$WORK_DIR/best_ips.txt"
  echo '```'
} > "$SYNC_DIR/README.md"

cd "$SYNC_DIR"
git add best_ips.txt full_ips.txt README.md
if ! git diff --cached --quiet; then
  git -c user.name="IP Update Bot" \
      -c user.email="ip-update-bot@users.noreply.github.com" \
      commit -m "Auto update best IPs"
  git "${GIT_PROXY_ARGS[@]}" push origin "$BRANCH" 2>&1 || { echo "ERROR: GitHub push failed" >&2; }
else
  echo "无变更，跳过推送"
fi
cd "$WORK_DIR"

# 清理 cron output（保留 2 小时）+ merge 日志保留 7 天
find "$LOG_DIR" \
  -type f -name '*.md' -mmin +120 -delete 2>/dev/null || true
find "$LOG_DIR" \
  -type f -name 'merge-history-????-??-??.log' -mtime +7 -delete 2>/dev/null || true