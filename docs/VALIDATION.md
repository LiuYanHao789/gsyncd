# gsyncd 验证报告

日期：2026-11-09。所有数据来自测试服务器（Anolis 7.9，8 核，62G，XFS 1.2T，底层为慢速虚拟机械盘）。**本机 NFS 为同一台机器上的模拟目标，不是跨机性能结论。**

## 1. 环境与数据集

- 磁盘：随机小文件负载约 11.7MB/s 读、13.8MB/s 写，`%util≈99.7%`，`await≈45ms`，`ROTA=1`。
- NFS：本机 nfs-server，v3，rsize/wsize=1MB；只能验证功能与粗略元数据成本。
- 数据形态：`pic/年/月/日/时/` 与单目录高密度两种；100KB～300KB 小文件为主。
- 测试数据实际占用峰值约 126GB，未超过 200GB 预算。

## 2. 构建与测试

| 项 | 结果 |
|---|---|
| `gofmt -l .` | 干净 |
| `go vet ./...` | 通过 |
| `go test ./... -count=1` | 全绿 |
| `CGO_ENABLED=1 go test -race ./internal/...` | 全绿，0 DATA RACE |
| `scripts/e2e.sh` | E2E OK |
| `scripts/smoke.sh` | SMOKE OK |
| 静态构建 | 2.76MB，statically linked |

## 3. 正确性

已修复并通过复现用例验证：

| 场景 | 修复前 | 修复后 |
|---|---|---|
| 写入后立即 rename | 目标新名字仍是旧内容，`errors=0` | 新内容正确 |
| 目录 rename 时子树内有 pending 写入/改名 | `missing=1 / extra=1 / mismatch=30` | 全部一致 |
| `rm -rf dir; mkdir dir` | 目标残留旧文件 | 旧文件清除 |
| 目标不存在 | 自动创建并写入 | exit 1，不创建 |
| 目标运行中被移走 | 重新创建路径继续写 | 拒绝写入、重试退避；恢复后收敛 |
| FIFO/特殊文件 | 可能永久挂起 | 跳过并记日志 |
| 目标 mtime 精度低（128B inode ext4） | checkpoint 永不命中、反复重拷、校验误报 | 按目标精度比较，命中正常 |

校验命令：

```bash
gsyncd -config ... -verify-tree          # 目录树/文件集合，目标端只 readdir
gsyncd -config ... -verify-content       # SHA-256 + size + mtime，全量
gsyncd -config ... -verify-content -verify-sample 10   # 抽样
```

实测：内容损坏 → `hash_mismatch=1`；mtime 改动 → `mtime_mismatch=1`；缺失/多余 → 对应计数；退出码非 0。

## 4. 性能

### 4.1 代码层（不受慢盘掩盖）

| 指标 | 修复前 | 修复后 |
|---|---|---|
| 10k 条目单目录 `readDirEntries` | 37.8ms | 17.8–21.6ms（约 2×） |
| 每 200 文件 `fcntl` | 1618 | 419 |
| 每 200 文件 `epoll_ctl` | 404（几乎全 EPERM） | 0 |
| ENOSPC 首传停止 | 27.8s | 2.38s |
| 聚合窗口 | 软上限，可无界增长 | `max_pending_hard` 默认 2M，超限 overflow |

> 类型切换安全增加的每文件 `Lstat(dst)` 是正确性成本；syscall 净减少仍显著。

### 4.2 端到端（环境瓶颈，仅作同环境相对比较）

| 场景 | 数据 | 耗时 | 文件/s | 峰值 RSS |
|---|---|---|---|---|
| 本地 20 万文件首传（100KB） | 20GB | 1524s | 131 | 34.9MB |
| 本地 20 万二次幂等 | 20GB | 196s | 1019 stat/s | 9.2MB |
| 本地 20 万 checkpoint 命中 | 8,400/8,400 dirs | 97.7s | 2046 | 9.3MB |
| 10k workers=1/2/4/8 | 1GB | 110.2/74.2/60.6/47.5s | 91/135/165/210 | 13–20MB |
| 10k 单目录 workers=1/8 | 1GB | 31.9/12.35s | 314/810 | 16–19MB |
| NFS 本机模拟 2 万首传 | 2GB | 151.2s | 132 | 27.6MB |
| NFS 本机模拟 2 万 checkpoint 命中 | 700/700 dirs | 6.16s | 3246 | 9.0MB |

传输时校验开销（10k，本地）：verify=false 43.1s（24% CPU）→ verify=true 50.3s（49% CPU），约 +16.8%；NFS 本机模拟 +2.4%（读回命中客户端缓存）。

校验命令成本（10k/1GB，每侧约 1GB）：

| 命令 | 本地 | 本机 NFS 模拟 |
|---|---|---|
| `-verify-tree` | 1.16s | 3.71s |
| `-verify-content` | 17.2s | 22.1s |

## 5. 稳定性

| 场景 | 结果 |
|---|---|
| kill -9 首传中断 | 5s 后重启：126 verified / 95 reused，32.2s 完成，tree/content 零差异 |
| 重复执行 | checkpoint 命中，0.18s（10k） |
| 目标移走再恢复 | 不重建路径、errors/retries 增长；恢复后 15s 收敛，零差异 |
| NFS server 停止再启动 | daemon 存活、D 状态、4.4% CPU（无忙等）；恢复后 1s 收敛 |
| 运行期磁盘满 | 初始 40 文件后快速失败；释放空间后全量恢复；tree/content 零差异 |
| 并发修改 | 覆盖写、append、删除重建、目录 rename + pending 写入：tree/content 零差异 |
| 目标 mtime 秒级精度 | checkpoint 1/1 reused；content mtime 零差异 |

## 6. 规模

| 场景 | 规模 | 首传 | 二次/checkpoint | 峰值 RSS |
|---|---|---|---|---|
| 单目录 10 万条目（1B 文件） | 100k | 32.2s（约 3.1k 文件/s） | 0.55s（1/1 reused） | 78.8MB |
| 单目录 50 万条目（1B 文件） | 500k | 213.8s（约 2.3k 文件/s） | 2.99s（1/1 reused） | 322MB |
| 单目录 10 万条目，verify-tree/content | 100k | — | 均 OK | — |
| 目录分布 20 万（100KB） | 20GB | 1524s | 97.7s checkpoint | 34.9MB |

结论：文件数主导元数据成本；单目录 50 万条目需要 322MB 用户态内存，生产上应按目录扇出和分片控制。当前 `readDirEntries` 仍一次性载入目录，流式 readdir 是后续优化项。

## 7. SLO 校准与 100TB 趋势

本机慢盘的数据带宽约 13MB/s，因此：
- 1TB 首传在本机约 21–22h（数据/元数据共同限制），不能作为生产 SLO；
- 生产 SSD/NVMe 上，1TB/500 万文件的目标应以基线相对提升和资源上限衡量，而非本机绝对时间；
- 代码层回归门槛建议：
  - 10k 条目 `readDirEntries` ≤ 25ms；
  - 每文件 fcntl ≤ 2、epoll_ctl = 0（Unix）；
  - 单目录 10 万条目 RSS ≤ 150MB，50 万 ≤ 400MB；
  - 500k checkpoint 二次启动 ≤ 5s；
  - ENOSPC 停止 ≤ 3s；
  - 校验/同步后 tree/content 差异为 0。

100TB（约 5 亿个 200KB 文件）只给趋势：
- 数据量 100TB；10GbE/本地 NVMe 下受带宽与 NFS RPC 限制，量级为天；
- 元数据：5 亿文件每次重启全量 stat 是数小时级；checkpoint 只省目标侧；
- 单目录 500 万文件不可取，应按目录分片；
- 瓶颈排序：目标 NFS 元数据 RPC > 单目录扇出/内存 > 源端 stat > 校验读回成本。

## 8. 已知边界

- 目标为 NFS 时，本报告只验证了同机模拟；跨机延迟/丢包/服务端并发需在生产环境复测。
- `-verify-content` 的读回可能命中客户端缓存；强一致需 `sync` 挂载或服务端校验。
- rsync/rsyncssh 依赖 rsync 自身块校验；逐文件读回校验仅 direct 后端。
- 目录属主/setuid/setgid/ACL/xattr 未完整保留；硬链接不保留 inode 关系。
- 单目录超大扇出仍是内存上限，流式 readdir 未实现。
- SHA-256 校验的 CPU 成本在高速存储上会更明显，xxhash64 为后续可选优化。

## 9. 提交与回滚

| 提交 | 内容 |
|---|---|
| `77c9abe` | P0 重命名/目录重建/目标守卫 |
| `aa7d507` | 传输时校验 + verify-tree/content |
| `49224cf` | 并行 stat、低开销 I/O、硬背压、ENOSPC 快速失败 |
| `e95359e` | 运行期目标身份校验 |
| `3c9bdda` | 目标时间戳精度适配 |

回滚：按提交 `git revert`；运行期可 `verify = false`、调大 `max_pending_hard`、关闭 `target_guard`（不推荐）。
