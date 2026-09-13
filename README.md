# Cloudflare IP 优选工具（个人分叉）

> 本仓库是 [HandsomeMJZ/cfip-tools](https://github.com/HandsomeMJZ/cfip-tools) 的个人分叉，用于在中国电信网络下自动优选 Cloudflare 边缘节点，结果推送至本仓库 `results` 分支，供 EdgeTunnel / Clash 类客户端订阅。

上游定位是「Windows 开箱即用工具」，本分叉的改造重心是 **Linux 常驻运行 + 对抗单次测速抖动**，代码与文档已按这个方向分叉。

---

## 分叉状态

| 项 | 值 |
| --- | --- |
| 上游 | [HandsomeMJZ/cfip-tools](https://github.com/HandsomeMJZ/cfip-tools) |
| 分叉基点 | 2026-05-09（上游 `972a7ae8` / 本仓库 `89b1648`） |
| 本分叉独有 | 12 个命令行参数、三阶段升级链路、历史节点池 |
| 未同步的上游提交 | 2026-05-23 的 6 个提交：Cloudflare R2 上传（`update.py` / `push_results.sh` / `start.sh`）、`windows_c/main.c` 改造、README 重写 |
| 未同步的上游参数 | `--show-latency`、`--show-mbps`（本分叉节点行只输出速度，见「输出格式」） |

未同步 R2 的原因：本分叉走自建三阶段 wrapper，不使用上游 `push_results.sh`；R2 的增量价值仅是绕开 `raw.githubusercontent.com` 的 CDN 缓存，代价是额外维护 R2 桶与客户端订阅地址，暂不需要。

---

## 一、运行链路

实际入口是三阶段 wrapper `cfip-updater.sh`（不在本仓库，位于运行机 `~/.hermes/scripts/cfip-updater/`）：

1. **测速**：`update.py --no-github-sync ...`
   只测速不推送。关键：`update.py` 内建推送，不加 `--no-github-sync` 会先把当次结果推上去，覆盖掉第 2 步的补位节点。
2. **合并**：`merge-history.py`
   历史节点池补位与驱逐，结果覆盖 `best_ips.txt`。
3. **推送**：统一 `git push`（`best_ips.txt` + `full_ips.txt` + 生成的 README → `results` 分支）。

阶段 2.5 是**空榜保护**：`best_ips.txt` 行数 < 5 时跳过推送并 `exit 0`。这是 2026-09-09 的真实事故补丁 —— 当时出口链路断流，TCP 可达数从 17,000+ 跌到 687，测出空榜直接覆盖了 GitHub 上的好榜。

其他工程细节：

- 测速阶段强制直连（wrapper 内 `unset *_PROXY`），避免代理干扰 `--resolve` 直连测速
- 推送阶段走代理：`-c http.proxy=... -c http.sslVerify=false`（修复 GnuTLS 握手失败）
- 运行期间**必须关闭 TUN 模式**，否则测速流量经代理通道，会快速消耗机场额度
- cron 调度：06:00–23:00 每小时一次（凌晨不跑，无更新）

生产调用参数：

```bash
python3 update.py --NO --top 10 --min-speed 12 --max-ports-per-ip 2 \
  --regions "JP,SG,KR,TW" --no-github-sync
```

---

## 二、历史节点池

**解决的问题**：优质节点（例如长期 17M+ 的那批）经常因单次 TCP 抖动滑出 per-region top10，当次订阅就丢了它。

三层保障：

| 机制 | 触发条件 | 关键参数 |
| --- | --- | --- |
| 补位 | 当次榜单缺席 + 池中**实时实测** ≥ 15M + TCP 存活 | `MIN_HISTORY_SPEED=15.0` |
| 健康驱逐 | 连续 3 轮池预检 TCP 失败（≈3 小时） | TCP 超时 1.5s，并发 50 |
| 每日复测 | 每 24h 全池 2MB 测速，连续 3 次 < 12M 降级驱逐（≈3 天） | `DEGRADE_THRESHOLD=12.0` / `DEGRADE_MAX=3` |

配套的保护与产能参数：

- 功勋容错：`MISS_BASE=3` / `MISS_PER_COUNT=2` / `MISS_MAX_CAP=8`（上榜次数越多容忍越久）
- 池容量上限 `MAX_POOL_SIZE=120`，超出淘汰功勋最低且不在当次榜的节点
- 网络故障保护：全池 TCP 存活率 < 30% 时跳过驱逐与 miss 累计；单轮降级比例 > 40% 熔断（`DEGRADE_MASS_RATIO`）
- 比例类判据带最小样本量 `MIN_RATIO_SAMPLE=8`，样本不足只记录不判定
- 驱逐档案 `eviction_history.json` 留档，节点重新入池时恢复历史功勋（`RESTORE_COUNT_CAP=16`），避免「驱逐 → 重新入池 → 功勋清零 → 再被驱逐」的 churn

**一条被实测否证的方案，留档避免重复提出**：曾想用「池内 `last_speed_check ≤ 6h` 且速度 ≥15M」的历史标签兜底劣化期的薄榜。A/B 实测（10 个 1.5 小时前刚实测 15-17M 的节点，串行单节点复测）只有 4-6M —— 旧标签 1.5 小时即失真。榜薄但真，不做假标签换长度。

---

## 三、本地新增参数（上游没有）

| 参数 | 默认 | 作用 |
| --- | --- | --- |
| `--regions` | 全部 | 地区过滤，如 `JP,SG,KR,TW`（排除 HK，电信线路到 HK 反而差） |
| `--max-ports-per-ip` | 2 | 同 IP 保留的端口数上限。上游源约 10% 的 IP 提供 26% 的节点（同一 IP 开满 2053/2083/2087/2096/443/8443），低质端口会挤占候选名额并干扰排序 |
| `--no-github-sync` | 关 | 禁用 `update.py` 内建推送，由 wrapper 统一推送 |
| `--github-repo` / `--github-branch` / `--github-path` / `--github-workdir` / `--github-message` | — | 内建推送的目标仓库 / 分支 / 路径 / 工作目录 / 提交信息 |
| `--github-token-env` | — | 存放 Token 的环境变量名（不从命令行明文传） |
| `--github-timeout` | 180 | git 命令超时 |
| `--git-http-proxy` / `--git-https-proxy` | — | 仅 git 走代理，测速仍直连 |

其余参数（`--top` / `--min-speed` / `--tcp-timeout` / `--tcp-workers` / `--speed-*` / `--NO` / `--verbose` / `-i` / `-o` / `--best-output`）与上游一致，`--help` 可查默认值。

`--max-ports-per-ip` 设 2 而非 1：避免误杀同 IP 双优端口（443 与 2083 同时 16M+ 的情况真实存在）。

---

## 四、输出与订阅

节点行格式：

```text
18.139.30.198:443#SG_1 [19.65M]
```

`IP:端口#地区_序号 [速度]`。上游新版会额外带延迟（`[45ms 28Mbps]`），本分叉暂未移植。

| 用途 | 地址 |
| --- | --- |
| 订阅（推荐） | `https://raw.githubusercontent.com/StevenSXY/cfbest/results/best_ips.txt` |
| 全量（供二次筛选） | `https://raw.githubusercontent.com/StevenSXY/cfbest/results/full_ips.txt` |

订阅说明：

- 客户端（Clash Verge 等）通过 proxy-provider 指向上面的 `best_ips.txt`，每小时自动生效
- ⚠️ `raw.githubusercontent.com` 有 5 分钟以上的 CDN 缓存，query buster 也绕不过。**验证推送结果请走 GitHub API**：

```bash
curl -s "https://api.github.com/repos/StevenSXY/cfbest/contents/best_ips.txt?ref=results" \
  | python3 -c "import json,base64,sys; print(len([l for l in base64.b64decode(json.load(sys.stdin)['content']).decode().splitlines() if l.strip()]),'节点')"
```

- 榜单长度有明确日节律：白天 06–10 点稳定 20–56 节点；晚 21–23 点 100% 低于 20 节点（近 30 天 57 轮统计，中位 22 节点）。**晚高峰薄榜是常态，不是故障**。
- 判定「是不是坏了」的顺序：先对日节律 → 再测榜内节点实际速度（薄榜 ≠ 不可用，曾出现全链路 5MB 直连仅 68KB/s 时榜内节点实测仍有 18.5M）

---

## 五、目录与文件

| 路径 | 说明 |
| --- | --- |
| `linux_py/update.py` | 测速主体（本仓库唯一在维护的代码） |
| `linux_py/ips.txt` | 本地 IP 源，下载失败时的回退 |
| `linux_py/best_ips.txt` | 优选结果（合并后的最终榜单） |
| `linux_py/full_ips.txt` | 全部测速成功节点 |
| `linux_py/start.sh` | 上游的单次运行脚本（本分叉由 wrapper 取代，保留原样） |
| `linux_py/push_results.sh` | 上游的推送脚本（本分叉不用，保留原样，**无 R2 逻辑**） |
| `windows_c/main.c` | 上游 Windows 版源码，本分叉不维护 |
| `linux_py/update_md.py` | 上游的 README 生成脚本 |

运行机上另有三份不在本仓库的脚本（见「运行链路」）：`cfip-updater.sh`（wrapper）、`merge-history.py`（历史池）、`analyze-history-pool.py`（池运行分析，可 `--days N`）。

---

## 六、注意事项

- **运行测速期间不要开 TUN 模式代理**，否则会快速消耗机场/KV 额度
- Token 只经环境变量（`.env`）注入，不写进代码、不写进配置文件、不提交到仓库
- 结果仓库分支是 `results`，代码在 `main`，两者不要混推
- 若推送报 `gnutls_handshake() failed`（exit 128）：测速与本地写结果都已成功，只是末段网络抖动，下一轮 cron 会重试，无需修复

---

## License

本项目仅供学习与个人使用。原始项目版权归 [HandsomeMJZ](https://github.com/HandsomeMJZ) 所有。
