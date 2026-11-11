# gsyncd

`gsyncd` 是 [lsyncd](https://github.com/lsyncd/lsyncd) 的 Go 重写版：一个单文件静态二进制、零系统依赖的实时目录镜像守护进程。默认后端自己拷贝文件，不依赖 Lua、rsync 或 ssh；也支持可选的 `rsync` / `rsyncssh` 后端。

```
$ gsyncd -version
gsyncd 0.4.0 (1.23.9)
```

## 特性

- 递归 inotify 监视（Linux）；其它平台使用快照轮询
- 事件聚合、`delay` 静默窗口（可选 lsyncd 风格的 `deadline`）、`max_hold` 强制刷新、可选 `close_write`
- direct 元数据保真：完整 mode、owner（root）、文件/目录 mtime、符号链接策略；可选 xattr/ACL、硬链接、稀疏、特殊文件
- 启动全量同步；`checkpoint` 断点续扫，重启不重扫目标端
- SIGHUP 优雅重载：排空在途批次后以同一 pid 重新执行，配置变更无需重启
- `rsyncssh` 的 move/delete 在远端本地执行（ssh），目录 rename 不重传
- 原子替换：临时文件 → fsync（大文件）→ rename
- 删除、重命名、排除规则；目录删除/重建和 rename 语义已加固
- 传输时 SHA-256 内容校验（默认开启）；`-verify-tree` / `-verify-content` 离线校验
- 目标守卫：目标未挂载/不存在时拒绝写入；运行期目标被移走也拒绝写入
- 特殊文件（FIFO/socket/device）跳过；符号链接仅保留指向同步根内部的链接
- 有界聚合窗口（软上限 + 硬上限）、退避重试、ENOSPC 快速失败

## 快速开始

```toml
# gsyncd.toml
[global]
log_file      = "/var/log/gsyncd.log"
log_level     = "info"
status_file   = "/var/lib/gsyncd/status.json"
max_processes = 8

[[sync]]
name    = "data"
source  = "/srv/data"
target  = "/backup/data"
mode    = "direct"
delay   = 15
delete  = true
excludes = ["*.tmp", "/cache/"]
```

```bash
# 目标盘必须先挂载；首次部署写一次 marker
gsyncd -config gsyncd.toml -init-target
gsyncd -config gsyncd.toml -check
gsyncd -config gsyncd.toml -nodaemon   # 前台运行（systemd 推荐）
gsyncd -config gsyncd.toml             # 后台运行
gsyncd -config gsyncd.toml -once       # 只做一次全量后退出
gsyncd -status /var/lib/gsyncd/status.json
```

## 命令行

| 参数 | 说明 |
|---|---|
| `-config PATH` | TOML 配置文件（必填，`-version`/`-status` 除外） |
| `-logfile PATH` | 覆盖配置里的 `log_file` |
| `-loglevel L` | `debug` \| `info` \| `warn` \| `error` \| `none` |
| `-pidfile PATH` | 覆盖 `pid_file` |
| `-statusfile PATH` | 覆盖 `status_file` |
| `-nodaemon` | 前台运行 |
| `-once` | 只做初始全量同步后退出 |
| `-status PATH` | 打印状态文件并退出 |
| `-check` | 校验配置、源目录和目标守卫后退出 |
| `-init-target` | 向每个本地目标写入 `.gsyncd-target` marker 后退出；目标必须已存在 |
| `-verify-tree` | 只读对比源/目标目录树和文件集合；有差异时退出码非 0 |
| `-verify-content` | 对比源/目标文件的 SHA-256、大小、mtime；有差异时退出码非 0 |
| `-verify-sample N` | 配合 `-verify-content`：按路径确定性抽取 N% 文件（0/100 = 全部） |
| `-verify-workers N` | 配合 `-verify-content`：并发哈希数（0 = `min(CPU,16)`） |
| `-watch-interval D` | 非 Linux 轮询间隔 |
| `-full-resync D` | 周期性全量重同步间隔；`-1` 表示沿用配置 |
| `-version` | 打印版本 |

`-verify-content` 会临时写一个探测文件判断目标文件系统的时间戳精度，随后删除。

## 配置

### `[global]`

| 键 | 默认 | 说明 |
|---|---|---|
| `log_file` | `""`（stderr） | 日志文件 |
| `log_level` | `info` | 日志级别 |
| `log_max_size` | `104857600` | 单个日志上限（字节） |
| `log_max_files` | `5` | 日志保留份数 |
| `pid_file` | `""` | pid 文件 |
| `status_file` | `""` | JSON 状态文件（原子替换） |
| `status_interval` | `10s` | 状态刷新间隔 |
| `max_processes` | `8` | 全局并发传输闸 |
| `max_pending` | `500000` | 聚合窗口软上限；达到即提前发送 |
| `max_pending_hard` | `2000000` | 聚合窗口硬上限；超过后丢弃新事件并全量恢复 |
| `watch_interval` | `30s` | 非 Linux 轮询间隔 |
| `full_resync` | `0` | 周期全量间隔；0 表示仅在 watch 装不下时自动启用 |
| `memory_limit` | `0` | Go 堆软上限，支持 `512m`/`1g` |
| `nice` | `0` | CPU nice 值 |
| `io_prio` | `""` | `idle` / `best-effort` / `realtime`，可带级别 |
| `target_guard` | `true` | 目标必须已存在且在非 `/` 挂载点或带 marker；否则拒绝启动 |

### `[[sync]]`

| 键 | 默认 | 说明 |
|---|---|---|
| `name` | 源目录 basename | 日志/状态标识 |
| `source` | — | 被监视的本地目录 |
| `target` | — | 目标目录；`rsyncssh` 下为目标主机 |
| `target_dir` | 源目录绝对路径 | 仅 `rsyncssh`：远端目标目录 |
| `mode` | `direct` | `direct` \| `rsync` \| `rsyncssh` |
| `delay` | `15s` | 事件静默多久后传输 |
| `delay_mode` | `"quiet"` | `"quiet"` 整树静默后刷；`"deadline"` 最早事件满 delay 即刷（lsyncd 语义） |
| `max_hold` | `15m` | 持续写入时最多等多久强制刷新；负值关闭 |
| `close_write` | `false` | 等待 writer 关闭后再同步 |
| `delete` | `true` | `true`/`false`/`"startup"`/`"running"` |
| `excludes` | `[]` | rsync 风格排除模式 |
| `exclude_from` | `[]` | 排除规则文件（相对配置文件目录），文件规则在前、内联规则在后 |
| `preserve_mode` | `"full"` | `"full"` 保留 setuid/setgid/sticky；`"basic"` 仅 0777 |
| `preserve_owner` | `"auto"` | `"auto"` 仅 root 时 chown；`true`/`false` |
| `symlink_mode` | `"safe"` | `"safe"` 仅根内链接；`"all"` 原样复制；`"skip"` |
| `preserve_xattrs` | `false` | Linux xattr（含 POSIX ACL），尽力而为 |
| `preserve_hardlinks` | `false` | 单次初始扫描内重建硬链接 |
| `sparse` | `false` | Linux 稀疏文件 |
| `copy_special` | `false` | Linux FIFO/设备节点（设备节点需要 root） |
| `bandwidth_limit` | `0` | direct 复制限速（字节/秒）；`rsync_*` 用 `rsync_bwlimit` |
| `full_resync` | `0` | 本 sync 的周期全量；0 继承全局 |
| `remote` | 自动推断 | 强制把含冒号的目标按 rsync 远端处理（覆盖自动推断） |
| `rsync_extra` | `[]` | 追加给 rsync 的参数（`rsyncopts` 是兼容别名） |
| `rsync_binary` / `rsync_bwlimit` / `rsync_compress` | — | 结构化 rsync 选项；见示例配置 |
| `rsync_xattrs` / `rsync_acls` / `rsync_hard_links` | `false` | rsync 元数据保真 |
| `rsync_chown` / `rsync_chmod` / `rsync_password_file` / `rsync_path` | — | rsync 透传选项 |
| `rsync_delete_excluded` | `false` | 全量传输时同时删除被排除的目标项 |
| `max_processes` | 0（用全局） | 该 sync 的并发上限 |
| `workers` | `min(CPU,16)` | direct 并行拷贝 goroutine 数 |
| `prefetch` | `min(CPU,16)` | direct 初始遍历预读目录的 goroutine 数 |
| `checkpoint` | `""` | direct 断点续扫记录文件；`"auto"` 自动选目录 |
| `batch_limit` | `10000` | 单次 rsync 最大路径数 |
| `fsync` | `false` | direct 每个文件 rename 前 fsync |
| `verify` | `true` | direct 传输时 SHA-256 校验 |
| `verify_retries` | `2` | 校验失败后的额外重拷次数 |

`[sync.ssh]` 支持 `binary`、`port`、`identity_file`、`options`。支持 snake_case 和 camelCase；未知键会报错。

## 校验

- **传输时校验（默认开）**：边拷边算 SHA-256，rename 前读回暂存文件复核；不一致重拷，仍失败则批次报错并记录路径。旧目标在 rename 前不被改动。
- **目录树**：`gsyncd -config ... -verify-tree`，目标端只 readdir，报告缺失/多余/类型不一致。
- **内容深校验**：`gsyncd -config ... -verify-content`，可加 `-verify-sample N`；两侧完整读一遍，代价较高，建议低频执行。
- **时间戳精度**：自动探测目标文件系统的 mtime 精度；128 字节 inode 的 ext4、FAT、部分 NFSv3 只能存整秒，比较时按该精度截断，避免误报与重复重拷。
- `rsync`/`rsyncssh` 依赖 rsync 自身块校验；逐文件读回校验仅 direct 后端。

## 目标守卫

`target_guard = true`（默认）时，启动前要求目标已存在，且位于非 `/` 挂载点或包含 `.gsyncd-target` marker。首次挂载后执行一次：

```bash
gsyncd -config /etc/gsyncd/gsyncd.toml -init-target
```

运行中目标被移走/替换时，批次会拒绝写入并退避重试；目标恢复后自动收敛。目标若被重新挂载为新 inode，需要重启 gsyncd。

## 与上游 lsyncd 的语义差异

迁移前请确认以下差异，它们都是有意设计，不是缺陷：

- **配置**：TOML 静态配置，不支持 Lua、Layer1~4、`onCreate/onStartup` 等任意 hook，也不支持 tunnel / crontab。需要外部编排时，请轮询 JSON 状态文件或由外部脚本触发。
- **`delay`**：默认是“整棵树静默 delay 后刷”，不是 lsyncd 的“每条事件到期即刷”。持续写入的树请设 `delay_mode = "deadline"`；`max_hold`（默认 15m）是永不静默时的兜底。
- **元数据**：direct 默认保留完整 mode、owner（仅 root）、文件/目录 mtime、符号链接；xattr/ACL、硬链接、稀疏文件、设备节点需要显式打开。目标根目录本身不 chown/chmod。`preserve_xattrs` 会禁用 checkpoint 的目录跳过：启动时逐文件核对属性（只写差异），大树上会增加一次元数据遍历。
- **符号链接**：默认 `safe`，指向同步根外部的链接不会被镜像；`all` 才是 lsyncd `-l` 的原样行为。
- **删除**：运行期删除由事件驱动，不主动扫描目标端多余项；目标端漂移由 Startup 或 `full_resync` 收敛。`delete = "running"` / `"startup"` 与 lsyncd 一致。重命名（`mv a b`）的旧名 `a` 总会从目标端移除，即使 `delete = false`/`"startup"`；这是 rename 语义，不是删除策略。
- **rsync / rsyncssh**：增量传输用 `--files-from` + 显式删除，不在增量批次上使用 `--delete`；`rsyncssh` 的 move/delete 通过 ssh 在远端本地执行，ssh 不可用时回退 rsync。`mode = "rsync"` 的远端目标删除走 `--delete-missing-args`。
- **重载**：SIGHUP 会先排空在途批次、刷新窗口，再以同一 pid 重新执行自身；配置变更不需要手动重启。TERM/INT 正常退出。
- **平台**：macOS/Windows 使用轮询兜底，没有 fsevents；大目录树请以 Linux 为主。

## 构建

```bash
make build            # 当前平台
bash scripts/dist.sh  # 仅 linux/amd64 与 linux/arm64 到 dist/
```

产物是静态单文件，`-buildvcs=false` 保证相同工具链下可复现。环境变量 `GSYNCD_RSYNC` 可覆盖 rsync 可执行文件路径。

## 测试

```bash
go test ./... -count=1
go test -race ./internal/... -count=1
bash scripts/e2e.sh
bash scripts/smoke.sh ./gsyncd
```

## 部署

- `examples/gsyncd.example.toml`：带注释的完整配置模板
- `deploy/gsyncd.service`：systemd unit（已使用 `-nodaemon`）
- `deploy/gsyncd.tmpfiles.conf`：`/var/lib/gsyncd`、`/var/log/gsyncd`
- `deploy/99-gsyncd.conf`：inotify/脏页/句柄调优

首次部署：挂载目标 → `-init-target` → `-check` → 启动服务。

## 许可证

GPL-2.0-or-later。
