#!/usr/bin/env python3
"""merge-history: cfip 当次测速 + 历史优质节点池合并（健康驱逐 + 每日速度复测）。

三层保障：
1. 补位： 当次缺席但历史速度 >= MIN_HISTORY_SPEED 且 TCP 存活的节点，以历史速度插入合并列表
2. 健康驱逐：每轮全池 TCP 预检，连续 MISS_MAX 轮失败即移除 —— 节点生死由实测决定，
   与上游 IP 源是否包含、时间窗口无关（不再有过期即删）
3. 每日速度复测：每 SPEED_CHECK_INTERVAL_H 小时全池真实测速，连续 DEGRADE_MAX 次
   低于 DEGRADE_THRESHOLD 才降级移除 —— 保证"活着且仍然优质"

2026-09-12 修复（原版由 v4-flash 生成，数据口径不可靠）：
- 复测改为节点级到期判定。旧版「任一节点缺 last_speed_check 就全池复测」+ update_pool 不写
  该字段 → 每小时都触发全池复测（实测 57/59 轮，3855 节点次 ≈ 7.5GB/3.3天），并把
  DEGRADE_MAX=3 的时间窗从 3 天压缩到 3 小时 → 3.3 天 264 次驱逐 / 159 唯一节点。
- 单轮降级比例 > DEGRADE_MASS_RATIO 熔断，防批量误清洗（实测有一轮 98 节点里 60 个降级，
  健康比例 39% 高于保护阈值 30% 未触发 → 一次性永久驱逐 40 个节点）。
- 驱逐前留档 (eviction_history.json)：重新入池时恢复历史 count，功勋不清零。
- 池容量上限 MAX_POOL_SIZE，超出淘汰功勋最低且不在当次榜的条目。

用法: python3 merge-history.py --best best_ips.txt --pool history_pool.json [--history eviction_history.json]
"""

import argparse
import asyncio
import json
import re
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

MIN_HISTORY_SPEED = 15.0    # 补位门槛（Mbps）
MISS_MAX = 3                # 基础容错轮数（新面孔 count<3）——从 MISS_BASE 推导, 保留兼容引用
MISS_BASE = 3               # 基础容错轮数（新面孔）
MISS_PER_COUNT = 2          # 每累计 2 次上榜, 容错 +1 轮（功勋制）
MISS_MAX_CAP = 8            # 容错轮数上限（防止真死节点被无限豁免）
SPEED_CHECK_INTERVAL_H = 24 # 全池速度复测间隔（小时）
DEGRADE_THRESHOLD = 12.0    # 复测低于此值视为降级（Mbps）
DEGRADE_MAX = 3             # 连续降级 N 次 → 移除（约 3 天）
TCP_TIMEOUT = 3.5           # TCP 预检超时（秒）——1.5s 在出口 RTT 劣化期(94-209ms)会大面积误判死亡,
                            # 导致池存活率假性 0-22% → 补位机制哑火(2026-09-10 实测)
TCP_WORKERS = 50            # TCP 预检并发
SPEED_WORKERS = 8           # 速度复测并发
SPEED_DOWNLOAD_BYTES = 10000000 # 复测下载量（10MB/节点，与 cfip 主测速口径统一）。
                                # 原 2MB 在 200ms RTT 下慢启动期占比过半, 实测低估 2.9~3.7 倍
                                # (2026-11-25: 47.131.106.251 2MB=16.53M vs 10MB=61.07M),
                                # 导致 DEGRADE_THRESHOLD=12M 把合格节点大面积误杀 → 补位候选全不达标。
                                # 代价: 50 节点 ≈ 500MB/轮, 3 轮/天 ≈ 1.5GB/天。
SPEED_TIMEOUT = 20          # 单节点复测超时（秒）；10MB 口径下 15s 偏紧
NETWORK_FAIL_RATIO = 0.30   # 全池 TCP 存活率低于此值 → 判定网络故障期, 跳过驱逐/miss 累计
DEGRADE_MASS_RATIO = 0.40   # 单轮复测降级比例高于此值 → 线路级劣化, 本轮不累计 degrade
                            # (2026-09-12 实测: 98 节点复测降级 60 个/健康比例 39%, 高于 30% 保护阈值,
                            #  未熔断 → 40 个节点被一次性永久驱逐)
MAX_POOL_SIZE = 120         # 池容量上限, 超出后淘汰功勋最低且不在当次榜的条目
RESTORE_COUNT_CAP = 6       # 重新入池时恢复的历史 count 上限。原 16 → miss 容错直接顶到上限 8 轮,
                            # 等于把「已被驱逐过的差节点」永久豁免 8 轮, 且每次驱逐都留档 →
                            # 驱逐机制实际失效(2026-11-25 实测 123.111.169.70 历史驱逐 3 次仍 count=17)。
                            # 6 → 容错 3+6//2=6 轮, 仍给优质回池节点缓冲, 但不再无上限洗活。
MIN_RATIO_SAMPLE = 8        # 比例类判据(健康比例/降级比例)的最少样本数, 样本不足时只记录不判定
SPEED_FAIL_MAX = 3          # 连续复测失败上限(测速失败驱逐)。与 DEGRADE_MAX 同为 3 次 ≈3 天。
                            # 存在的理由: TCP 存活但 HTTPS 测速持续失败的节点, 旧版既不扣 miss
                            # (TCP 活着) 也不累计 degrade(没有速度值) → 永久卡在池里。
                            # 2026-09-14 实测 134.185.123.54:443 卡 75h, miss=0 degrade=0。
SPEED_FAIL_MASS_RATIO = 0.50  # 单轮测速失败比例熔断: 链路级故障时不清池

LINK_SLOW_FACTOR = 0.25     # 链路降级期补位/兜底的相对阈值倍率:
                            # 阈值 = min(DEGRADE_THRESHOLD, 本轮复测中位数 × 倍率)
                            # 依据: 12:06 健康比例 0% 时硬阈值 12M 把 15 个 20M 级节点全判出局
                            # (06:01 同批节点实测 19-21M), 榜从 16 条塌到 2 条。
                            # 0.25 = 环境整体慢 4 倍时仍只放行该环境中相对最快的头部节点。
ABS_MIN_SPEED = 4.0         # 入榜绝对下限 (Mbps), 不随链路降级缩放:
                            # 榜是给用户直接用的, 低于 4M 的节点入库只会把连接导到更差路径。
                            # 实测: 链路降级期相对阈值 1.0M 会放行 1.06M 的节点进榜。

POOL_FILL_TARGET = 15       # 榜条数低于此值时启用「池兜底补厚」(阶段 C2)
                            # 依据: 2026-11-08 12:06 单轮噪声只出 2 条上榜, 而同池 57 个 ≥12M;
                            # 老逻辑没有兜底, 单轮测速失真 = 全网榜单失真。
POOL_FILL_MAX = 20          # 单轮兜底最多测几个 (每个 2MB, 上限 40MB/轮) —— 控制流量成本

LINE_RE = re.compile(r"^([\d\.]+):(\d+)#([A-Z]+)_(\d+)\s+\[([\d\.]+)M(?:\s+cv([\d\.]+))?\]")


def parse_best(path: Path):
    """解析 best_ips.txt → [(ip, port, region, idx, speed_mbps, cv_pct|None)]"""
    rows = []
    if not path.exists():
        return rows
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        m = LINE_RE.match(line)
        if not m:
            continue
        ip, port, region, idx, speed, cv = m.groups()
        rows.append((ip, int(port), region, int(idx), float(speed),
                     float(cv) if cv else None))
    return rows


def load_pool(path: Path):
    """加载历史池（无时间过期，仅结构校验）"""
    try:
        return json.loads(path.read_text()) if path.exists() else {}
    except Exception:
        return {}


def pool_items(pool):
    """池 → [(key, ip, port, region, speed)]"""
    items = []
    for key, rec in pool.items():
        ip, port = key.rsplit(":", 1)
        items.append((key, ip, int(port), rec.get("region", ""), rec.get("speed", 0.0)))
    return items


def load_history(path: Path):
    """加载驱逐历史(功勋档案): key → {evictions, max_count, last_reason, last_ts}"""
    try:
        return json.loads(path.read_text()) if path.exists() else {}
    except Exception:
        return {}


def save_history(path: Path, hist):
    try:
        path.write_text(json.dumps(hist, ensure_ascii=False, indent=1))
    except Exception as e:
        print(f"[merge-history] ⚠️ 驱逐历史写入失败: {e}")


def record_eviction(hist, pool, keys, reason, now):
    """驱逐前留档: 保留节点功勋(count)与原因。
    2026-09-12 实测问题: 被驱逐节点若仍在当次榜, update_pool 会重新入池且 count=1,
    功勋容错清零 → 下轮再被驱逐, 3.3 天出现 264 次驱逐 / 159 个唯一节点(单节点最多 7 次)。"""
    for k in keys:
        rec = hist.setdefault(k, {"evictions": 0, "max_count": 0, "last_reason": "", "last_ts": ""})
        rec["evictions"] = rec.get("evictions", 0) + 1
        rec["max_count"] = max(int(rec.get("max_count", 0)), int(pool.get(k, {}).get("count", 0)))
        rec["last_reason"] = reason
        rec["last_ts"] = now.isoformat(timespec="minutes")


def speed_check_due(rec, now):
    """节点级复测到期判定(替代旧版「任一节点缺失就越级全池复测」)"""
    last = rec.get("last_speed_check")
    if not last:
        return True
    try:
        return now - datetime.fromisoformat(last) >= timedelta(hours=SPEED_CHECK_INTERVAL_H)
    except Exception:
        return True


def update_pool(pool, rows, hist=None):
    """当次结果刷新池：同 key 更新 speed/ts/count 并重置 miss/degrade, 同时刷新 last_speed_check
    —— 主测速已经真实测过这些节点, 不打标会让它们下一轮被全池复测重复测一遍
    (2026-09-12 根因: 旧版新条目不写 last_speed_check → 每小时都有新节点入池 →
     24h 复测间隔形同虚设, 实测 57/59 轮触发全池复测)。"""
    now = datetime.now()
    now_s = now.isoformat(timespec="minutes")
    for ip, port, region, _idx, speed, _cv in rows:
        key = f"{ip}:{port}"
        if key in pool:
            pool[key].update({
                "speed": speed,
                "ts": now_s,
                "count": pool[key].get("count", 0) + 1,
                "miss": 0,
                "degrade": 0,
                "speed_fail": 0,   # 上了当次榜 = 实测可用, 失败计数清零
                "last_speed_check": now_s,
            })
        else:
            hrec = hist.get(key, {}) if hist else {}
            restored = min(int(hrec.get("max_count", 0)), RESTORE_COUNT_CAP)
            pool[key] = {
                "speed": speed, "ts": now_s, "region": region,
                "count": 1 + restored, "miss": 0, "degrade": 0,
                "speed_fail": 0,
                "last_speed_check": now_s,
            }
            if restored:
                print(f"[merge-history] ♻️ {key} 重新入池, 恢复功勋 count={1 + restored} "
                      f"(历史驱逐 {hrec.get('evictions', 0)} 次)")
    return pool


async def tcp_alive(ip: str, port: int, timeout: float):
    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_connection(ip, port), timeout=timeout
        )
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return True
    except Exception:
        return False


async def filter_alive(items, timeout=TCP_TIMEOUT, workers=TCP_WORKERS):
    """并发 TCP 预检，返回 (item, alive) 列表"""
    sem = asyncio.Semaphore(workers)

    async def check(item):
        async with sem:
            ok = await tcp_alive(item[1], item[2], timeout)
            return (item, ok)

    results = await asyncio.gather(*(check(i) for i in items))
    return results


async def speed_test_one(item, timeout=SPEED_TIMEOUT):
    """单节点真实测速（curl --resolve 直连，--noproxy 保证不走代理）→ Mbps 或 None"""
    _, ip, port, _region, _speed = item
    cmd = [
        "curl", "-s", "--noproxy", "*",
        "--resolve", f"speed.cloudflare.com:{port}:{ip}",
        f"https://speed.cloudflare.com/__down?bytes={SPEED_DOWNLOAD_BYTES}",
        "-o", "/dev/null", "-w", "%{speed_download}",
        "--max-time", str(timeout),
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 2)
        bps = float(r.stdout.strip())
        if bps <= 0:
            return None
        return bps * 8 / 1_000_000
    except Exception:
        return None


async def speed_test_all(items, workers=SPEED_WORKERS):
    """全池并发测速 → {key: mbps}（失败为 None 不收录）"""
    sem = asyncio.Semaphore(workers)

    async def check(item):
        async with sem:
            mbps = await speed_test_one(item)
            return (item[0], mbps)

    results = await asyncio.gather(*(check(i) for i in items))
    return {key: mbps for key, mbps in results if mbps is not None}


def miss_limit(rec):
    """功勋容错: 基础 3 轮 + 每 2 次上榜 +1 轮, 上限 8 轮。
    count 越高(经过越多轮实测验证) → 容错越大, 优质节点免受单次抖动误杀。"""
    count = rec.get("count", 0)
    return min(MISS_BASE + count // MISS_PER_COUNT, MISS_MAX_CAP)


def quality_mark(cv, speed, rec):
    """质量标记: 让「优质节点」在 Clash/小火箭列表里一眼可辨。

    2026-11-26 实测依据: 同一批节点连测 3 轮, CV 中位 15.1%, 4/16 节点 CV>25%,
    单节点三轮速度可差 51% —— 速度数字本身不足以判断质量, 必须配 CV。

    判据 (优先用当轮双测算出的 CV, 无 CV 时退回池内历史):
      ★   CV < 10% 且 speed > 25M     → 又快又稳, 首选
      ☆   CV >= 10% 且 speed > 25M    → 快但不稳
      (无) 其余(12~25M 或 无 CV 数据)  → 普通可用
      ⚠   rec 有 miss/degrade/speed_fail 异常记录

    为什么不用旧的 count 数字: count 是「累计上榜次数」, 手动测试也会让它 +1,
    曾被误读为「连续 N 轮稳定」(★25 实为 8 天前的累计值)。
    """
    if rec:
        abnormal = (int(rec.get("miss", 0)) + int(rec.get("degrade", 0))
                    + int(rec.get("speed_fail", 0)))
        if abnormal > 0:
            return " ⚠"
    if cv is not None and speed > 25.0:
        return " ★" if cv < 10.0 else " ☆"
    return ""


def render_row(ip, port, region, speed, idx, rec=None, cv=None):
    mark = quality_mark(cv, speed, rec)
    return f"{ip}:{port}#{region}_{idx} [{speed:.2f}M]{mark}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--best", required=True, type=Path)
    ap.add_argument("--pool", required=True, type=Path)
    ap.add_argument("--history", type=Path, default=None,
                    help="驱逐历史(功勋档案), 默认与池同目录 eviction_history.json")
    args = ap.parse_args()

    history_path = args.history or (args.pool.parent / "eviction_history.json")
    current = parse_best(args.best)
    pool = load_pool(args.pool)
    hist = load_history(history_path)
    now = datetime.now()

    # ── 阶段 A: 健康驱逐（全池 TCP 预检，连续失败才移除）──
    items = pool_items(pool)
    if items:
        probe_results = asyncio.run(filter_alive(items))
        alive_keys = {it[0] for it, ok in probe_results if ok}
        survive_ratio = len(alive_keys) / len(items)

        # 网络故障保护(2026-09-09 19:00 实测: 上游出口断流时全池 TCP 大面积失败,
        # 当日霸主 211.110.208.116 等被误驱逐; 恢复后它们其实是好节点)。
        # 全池存活率异常低 → 判定链路故障期, 本轮不驱逐、不累计 miss, 只记录。
        if survive_ratio < NETWORK_FAIL_RATIO:
            print(f"[merge-history] ⚠️ 网络故障保护: 全池 TCP 存活率 {survive_ratio:.0%} (< {NETWORK_FAIL_RATIO:.0%}), "
                  f"跳过驱逐与 miss 累计, 等待下轮恢复")
            removed_dead = []
        else:
            removed_dead = []
            for it, ok in probe_results:
                key = it[0]
                if ok:
                    pool[key]["miss"] = 0
                else:
                    pool[key]["miss"] = pool[key].get("miss", 0) + 1
                    limit = miss_limit(pool[key])
                    if pool[key]["miss"] >= limit:
                        removed_dead.append(key)
                    else:
                        print(f"[merge-history]   {key} TCP 失败 {pool[key]['miss']}/{limit} 轮 (未达容错)")
            for key in removed_dead:
                record_eviction(hist, pool, [key], "health", now)
                del pool[key]
            if removed_dead:
                print(f"[merge-history] 🪦 健康驱逐 {len(removed_dead)} 个真死节点: {removed_dead}")
        print(f"[merge-history] 池存活 {len(pool)}/{len(items)} (功勋容错: 每2次上榜+1轮, 上限 {MISS_MAX_CAP} 轮)")
    else:
        alive_keys = set()

    # 链路降级标记: 阶段 B 有足够样本且健康比例过低时置 True, 供阶段 C/C2 放宽容差
    link_degraded = False
    link_floor = float(DEGRADE_THRESHOLD)   # 本轮生效的补位/兜底阈值下限

    # ── 阶段 B: 每日速度复测（降级驱逐）──
    # 2026-09-12 修复: 旧版「池中任一节点缺 last_speed_check 就全池复测」+ 新条目不写该字段
    # → 每小时都触发全池复测(实战 57/59 轮), 3855 节点次/3.3 天 ≈ 7.5GB, 且使 DEGRADE_MAX=3
    # 的时间窗从 3 天塌缩到 3 小时 → 3.3 天 264 次驱逐/159 唯一节点。改为按节点到期只测超期节点。
    items_now = pool_items(pool)
    due_items = [it for it in items_now if speed_check_due(pool[it[0]], now)]

    if due_items:
        print(f"[merge-history] ⏱️ 每日速度复测: {len(due_items)}/{len(items_now)} 节点到期, 测速中...")
        test_items = [it for it in due_items if it[0] in alive_keys]
        speeds = asyncio.run(speed_test_all(test_items))
        healthy_count = sum(1 for v in speeds.values() if v >= DEGRADE_THRESHOLD)
        healthy_ratio = healthy_count / max(len(test_items), 1)
        deg_this_round = sum(1 for v in speeds.values() if v < DEGRADE_THRESHOLD)
        deg_ratio = deg_this_round / max(len(speeds), 1)
        degraded = []
        skip_reason = None
        # 网络故障保护(与阶段A同源): 复测成功率过低 → 测速结果不可信,
        # 不累计 degrade(否则 19:00 故障期的全失败会把优质节点3次标降级驱逐)。
        # 判据用「健康速度比例」而非「成功率」: 故障时 curl 返回 0.1M 也"成功",
        # 成功率看似正常, 但健康(≥DEGRADE_THRESHOLD)比例暴跌 → 这才是故障信号。
        # ⚠️ 比例判据仅在样本充足时生效: 按节点到期复测后单轮常只剩几个节点,
        # 4 个里 1 个慢 = 健康比例 25% 会误触发保护(2026-09-12 首次真实运行实测到)。
        if test_items and len(test_items) >= MIN_RATIO_SAMPLE and healthy_ratio < NETWORK_FAIL_RATIO:
            skip_reason = (f"健康速度比例 {healthy_ratio:.0%} ({healthy_count}/{len(test_items)}) "
                           f"< {NETWORK_FAIL_RATIO:.0%}")
        elif len(speeds) >= MIN_RATIO_SAMPLE and deg_ratio > DEGRADE_MASS_RATIO:
            # 单轮批量降级熔断: 保护阈值(30%)太松, 实测出现过健康比例 39% 仍被判正常 →
            # 一次性永久驱逐 40 个节点的批量误清洗。
            skip_reason = (f"单轮降级比例 {deg_ratio:.0%} ({deg_this_round}/{len(speeds)}) "
                           f"> {DEGRADE_MASS_RATIO:.0%}")
        if skip_reason:
            print(f"[merge-history] ⚠️ 网络故障保护: {skip_reason}, 本轮不累计 degrade (仅刷新速度标签)")
        for key in list(pool.keys()):
            if key not in speeds:
                continue
            pool[key]["speed"] = speeds[key]
            pool[key]["last_speed_check"] = now.isoformat(timespec="minutes")
            if not skip_reason:
                if speeds[key] < DEGRADE_THRESHOLD:
                    pool[key]["degrade"] = pool[key].get("degrade", 0) + 1
                    if pool[key]["degrade"] >= DEGRADE_MAX:
                        degraded.append(key)
                else:
                    pool[key]["degrade"] = 0
            pool[key]["speed_fail"] = 0   # 测出成绩即清零(无论本轮是否熔断)
            print(f"  {key}: {speeds[key]:.1f}M {'⚠️降级' if speeds[key] < DEGRADE_THRESHOLD else '✅'}")

        # ⚠️ 健康度快照(2026-11-08): 复测与补位/兜底用同一口径(2MB / speed.cloudflare.com),
        # 所以「复测健康比例」直接反映本轮测速环境是否可信。补位/兜底必须据此决定尺度:
        # 12:06 那轮 9 个到期节点健康比例 0%, 而补位仍拿 12M 硬阈值判 15 个节点全部出局 →
        # 池里有 57 个 18.6M 中位节点, 榜单只剩 2 条。
        # 判据带 MIN_RATIO_SAMPLE: 样本不足(按节点到期常只 3-5 个)时不下结论, 保持老行为。
        link_degraded = bool(
            len(speeds) >= MIN_RATIO_SAMPLE
            and (sum(1 for v in speeds.values() if v >= DEGRADE_THRESHOLD) / len(speeds)) < NETWORK_FAIL_RATIO
        )
        if link_degraded:
            n_ok = sum(1 for v in speeds.values() if v >= DEGRADE_THRESHOLD)
            pos = sorted(v for v in speeds.values() if v > 0)
            med = pos[len(pos) // 2] if pos else 0.0
            link_floor = min(float(DEGRADE_THRESHOLD), round(med * LINK_SLOW_FACTOR, 2))
            print(f"[merge-history] 🧪 链路降级期: 复测健康比例 {n_ok}/{len(speeds)} < {NETWORK_FAIL_RATIO:.0%}, "
                  f"补位/兜底阈值 {DEGRADE_THRESHOLD:.0f}M → {link_floor:.1f}M "
                  f"(本轮中位 {med:.1f}M × {LINK_SLOW_FACTOR})")
        if degraded:
            record_eviction(hist, pool, degraded, "degrade", now)
            for key in degraded:
                del pool[key]
            print(f"[merge-history] 🪦 降级驱逐 {len(degraded)} 个: {degraded}")

        # ── 阶段 B2: 测速失败(存活但测不出成绩)──
        # 2026-09-14 新增。旧版对失败节点直接 continue: last_speed_check 不更新、
        # degrade 不累计、且不留任何日志 → TCP 层可达但 HTTPS 测速长期失败的节点
        # 掉进「健康驱逐」与「降级驱逐」的缝里, 永久占据池位(实测卡 75h)。
        # 现在: 失败也刷新 last_speed_check(语义=已尝试) + 独立计数, 连续 SPEED_FAIL_MAX 次驱逐。
        tested_keys = [it[0] for it in test_items]
        failed_keys = [k for k in tested_keys if k not in speeds]
        if failed_keys:
            fail_ratio = len(failed_keys) / max(len(tested_keys), 1)
            # 熔断: 大面积失败是链路级故障, 不是节点问题(同阶段A/B的保护思路)
            fail_skip = None
            if len(tested_keys) >= MIN_RATIO_SAMPLE and fail_ratio > SPEED_FAIL_MASS_RATIO:
                fail_skip = (f"单轮测速失败比例 {fail_ratio:.0%} ({len(failed_keys)}/{len(tested_keys)}) "
                             f"> {SPEED_FAIL_MASS_RATIO:.0%}")
                print(f"[merge-history] ⚠️ 网络故障保护: {fail_skip}, 本轮不累计 speed_fail")
            speed_failed = []
            for key in failed_keys:
                pool[key]["last_speed_check"] = now.isoformat(timespec="minutes")
                if fail_skip:
                    print(f"  {key}: 测速失败 ❌ (保护期, 不累计)")
                    continue
                pool[key]["speed_fail"] = pool[key].get("speed_fail", 0) + 1
                print(f"  {key}: 测速失败 ❌ 连续 {pool[key]['speed_fail']}/{SPEED_FAIL_MAX}")
                if pool[key]["speed_fail"] >= SPEED_FAIL_MAX:
                    speed_failed.append(key)
            if speed_failed:
                record_eviction(hist, pool, speed_failed, "speed_fail", now)
                for key in speed_failed:
                    del pool[key]
                print(f"[merge-history] 🪦 测速失败驱逐 {len(speed_failed)} 个: {speed_failed}")
    else:
        print(f"[merge-history] 速度复测未到期（每 {SPEED_CHECK_INTERVAL_H}h 一次, "
              f"{len(items_now)} 节点均新鲜）")

    # ── 阶段 C: 补位合并（补位候选实时测速，不用历史速度标签）──
    current_keys = {f"{ip}:{port}" for ip, port, _r, _i, _s, _cv in current}
    # 同 IP 多端口去重:每个 IP 只保留速度最高的一个补位候选。
    # 上游源头常同 IP 开满 6 端口,若全补位则低质端口(8443=1M)混入订阅
    # 干扰 fallback 排序;同 IP 保留最快端口即可覆盖该 IP 的可用性。
    best_by_ip: dict[str, tuple] = {}
    for key, ip, port, region, speed in pool_items(pool):
        if speed >= MIN_HISTORY_SPEED and key not in current_keys and key in alive_keys:
            prev = best_by_ip.get(ip)
            if prev is None or speed > prev[4]:
                best_by_ip[ip] = (key, ip, port, region, speed)
    candidates = list(best_by_ip.values())

    revived = []
    if candidates:
        print(f"[merge-history] ⏱️ 补位候选 {len(candidates)} 个，实时测速中...")
        cand_speeds = asyncio.run(speed_test_all(candidates))
        # 补位样本同样可判定链路降级(与兜底同理): 40 个候选里若健康比例过低,
        # 说明本轮国际出口整体慢, 12M 硬阈值会把好节点全判死。
        cand_floor = link_floor
        pos_cand = sorted(v for v in cand_speeds.values() if v > 0)
        if pos_cand and len(pos_cand) >= MIN_RATIO_SAMPLE:
            c_ok = sum(1 for v in pos_cand if v >= DEGRADE_THRESHOLD)
            if c_ok / len(pos_cand) < NETWORK_FAIL_RATIO:
                c_med = pos_cand[len(pos_cand) // 2]
                cand_floor = min(float(DEGRADE_THRESHOLD), round(c_med * LINK_SLOW_FACTOR, 2))
                print(f"[merge-history] 🧪 补位样本判定链路降级: 健康 {c_ok}/{len(pos_cand)} "
                      f"< {NETWORK_FAIL_RATIO:.0%}, 阈值 → {cand_floor:.1f}M (中位 {c_med:.1f}M)")
        for key, ip, port, region, _old_speed in candidates:
            cur = cand_speeds.get(key)
            if cur is not None and cur >= cand_floor and cur >= ABS_MIN_SPEED:
                revived.append((ip, port, region, cur, None))   # 补位无 CV 数据
                pool[key]["speed"] = cur              # 实时速度回写池
                pool[key]["degrade"] = 0
                pool[key]["last_speed_check"] = datetime.now().isoformat(timespec="minutes")
                print(f"  {ip}: {cur:.1f}M ✅ 补位")
            else:
                print(f"  {ip}: {cur:.1f}M ❌ 未达阈值或失败，不补位" if cur else f"  {ip}: 测速失败 ❌")
        if revived:
            print(f"[merge-history] ✅ 补位 {len(revived)} 个（实时速度）")
    else:
        print(f"[merge-history] 无补位候选")

    # merged_rows 元素: (ip, port, region, speed, cv|None) —— cv 用于打质量标记
    merged_rows = [(ip, port, region, speed, cv)
                   for ip, port, region, _idx, speed, cv in current]
    merged_rows.extend(revived)

    # ── 阶段 C2: 池兜底补厚 (2026-11-08) ──
    # 症状: 12:06 那轮测速环境异常 (50 候选只出 2 个 fast), 单轮噪声直接决定了全网榜单;
    # 而池里同期有 57 个 ≥12M 的节点, 中位 18.6M —— 榜薄不是没节点, 是没用上池。
    # 做法: 当次+补位仍不足 POOL_FILL_TARGET 条时, 从池内挑「非当次、非已补位」的节点,
    # 按需实时测速 (而不是信历史标签 —— 见 skill「补位必须实时测速」) 达阈值才入榜。
    # 上限 POOL_FILL_MAX 控制实时测速成本 (每多测 1 个 ≈ 2MB)。
    fill_added = []
    have_keys = {f"{ip}:{port}" for ip, port, _r, _s, _cv in merged_rows}
    if len(merged_rows) < POOL_FILL_TARGET:
        need = min(POOL_FILL_TARGET - len(merged_rows), POOL_FILL_MAX)
        fill_by_ip: dict[str, tuple] = {}
        for key, ip, port, region, speed in pool_items(pool):
            if key in have_keys or key not in alive_keys:
                continue
            if speed < MIN_HISTORY_SPEED:
                continue
            prev = fill_by_ip.get(ip)
            if prev is None or speed > prev[4]:
                fill_by_ip[ip] = (key, ip, port, region, speed)
        fill_cands = sorted(fill_by_ip.values(), key=lambda x: -x[4])[:need]
        if fill_cands:
            print(f"[merge-history] 🧩 榜仅 {len(merged_rows)} 条 (< {POOL_FILL_TARGET}), "
                  f"池兜底测速 {len(fill_cands)} 个...")
            fill_speeds = asyncio.run(speed_test_all(fill_cands))
            # 兜底自身样本也是「本轮链路快慢」的证据: 到期复测常只有 3-5 个节点、
            # 不足以触发 link_degraded, 但兜底这批 10-20 个足以定性。
            fill_floor = link_floor
            pos_fill = sorted(v for v in fill_speeds.values() if v > 0)
            if pos_fill:
                f_ok = sum(1 for v in pos_fill if v >= DEGRADE_THRESHOLD)
                f_ratio = f_ok / len(pos_fill)
                if len(pos_fill) >= MIN_RATIO_SAMPLE and f_ratio < NETWORK_FAIL_RATIO:
                    f_med = pos_fill[len(pos_fill) // 2]
                    fill_floor = min(float(DEGRADE_THRESHOLD), round(f_med * LINK_SLOW_FACTOR, 2))
                    print(f"[merge-history] 🧪 兜底样本判定链路降级: 健康 {f_ok}/{len(pos_fill)} "
                          f"< {NETWORK_FAIL_RATIO:.0%}, 阈值 → {fill_floor:.1f}M (中位 {f_med:.1f}M)")
            # 兜底: 绝对下限 (不随链路缩放) —— 榜单最终服务于「能用」, 太慢的节点
            # 入榜只会把用户的连接导入更差的路径。相对阈值只在 ≥ABS_FLOOR 之上起作用。
            for key, ip, port, region, _old in fill_cands:
                cur = fill_speeds.get(key)
                if cur is not None and cur >= fill_floor and cur >= ABS_MIN_SPEED:
                    fill_added.append((ip, port, region, cur, None))   # 兜底无 CV 数据
                    pool[key]["speed"] = cur
                    pool[key]["degrade"] = 0
                    pool[key]["speed_fail"] = 0
                    pool[key]["last_speed_check"] = datetime.now().isoformat(timespec="minutes")
                    print(f"  {ip}: {cur:.1f}M ✅ 兜底入榜")
                else:
                    print(f"  {ip}: {cur:.1f}M ❌ 兜底未达标" if cur else f"  {ip}: 兜底测速失败 ❌")
            if fill_added:
                merged_rows.extend(fill_added)
                print(f"[merge-history] ✅ 兜底补厚 {len(fill_added)} 个")

    merged_rows.sort(key=lambda r: -r[3])

    region_counters = {}
    lines = []
    for ip, port, region, speed, cv in merged_rows:
        region_counters[region] = region_counters.get(region, 0) + 1
        rec = pool.get(f"{ip}:{port}")
        lines.append(render_row(ip, port, region, speed, region_counters[region], rec, cv))

    args.best.write_text("\n".join(lines) + "\n")
    print(f"[merge-history] ✅ 合并输出: 当次 {len(current)} + 补位 {len(revived)} = {len(lines)} 节点")

    pool = update_pool(pool, current, hist)

    # ── 阶段 D: 池容量上限（池一度涨到 98 条, 池越大补位候选越多 → 放大实时测速成本）──
    if len(pool) > MAX_POOL_SIZE:
        prunable = sorted(
            (k for k in pool if k not in current_keys),
            key=lambda k: (pool[k].get("count", 0), pool[k].get("speed", 0.0)),
        )
        drop = prunable[: len(pool) - MAX_POOL_SIZE]
        if drop:
            record_eviction(hist, pool, drop, "cap", now)
            for k in drop:
                del pool[k]
            print(f"[merge-history] 🗑️ 池超上限 {MAX_POOL_SIZE}, 淘汰 {len(drop)} 条功勋最低条目: {drop}")

    args.pool.write_text(json.dumps(pool, ensure_ascii=False, indent=1))
    save_history(history_path, hist)
    return 0


if __name__ == "__main__":
    sys.exit(main())