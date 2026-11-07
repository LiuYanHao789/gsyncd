# gsyncd

`gsyncd` 是 [lsyncd](https://github.com/lsyncd/lsyncd) 的 Go 重写版：一个**单文件静态二进制**、**零系统依赖**的实时目录镜像守护进程。

原版需要 Lua 解释器 + 编译期依赖 + 运行期的 rsync/ssh；`gsyncd` 不需要这些：默认后端自己拷文件，可选地仍然调用 rsync/rsyncssh 以兼容既有部署。

```
$ gsyncd -version
gsyncd 0.1.22 (1.23.4)
```

## 快速开始

```toml
# gsyncd.toml
[global]
log_file     = "/var/log/gsyncd.log"
log_level    = "info"
status_file  = "/var/lib/gsyncd/status.json"
max_processes = 8

[[sync]]
name   = "data"
source = "/srv/data"
target = "/backup/data"
mode   = "direct"
delay  = 15
delete = true
excludes = ["*.tmp", "/cache/"]
```

```
$ gsyncd -config gsyncd.toml -nodaemon     # 前台运行
$ gsyncd -config gsyncd.toml               # 后台运行
$ gsyncd -config gsyncd.toml -check        # 只校验配置
$ gsyncd -config gsyncd.toml -once         # 只做一次全量同步后退出
$ gsyncd -status /var/lib/gsyncd/status.json
```

## 目录结构

```
gsyncd/
├── cmd/
│   ├── gsyncd/            CLI 入口:flag 解析、信号、pidfile、-check/-status/-once、后台化
│   ├── copybench/         拷贝吞吐基准(用于比较 buffer 大小与后端)
│   └── probemark/         内核 watch 内存与装表速率探针(仅 Linux)
├── internal/
│   ├── config/            TOML 子集解析器(自写,零依赖)+ 配置校验
│   ├── engine/            编排:每 sync 一个聚合窗口、delay 定时器、传输闸、重试回填
│   ├── aggregate/         事件折叠(map[path]index,每事件 O(1))
│   ├── filter/            排除模式编译与匹配(字符类、**、锚定、取反)
│   ├── sync/              后端:direct / rsync / rsyncssh + 退避重试 + 断点续扫
│   ├── watcher/           监视后端:inotify(Linux)/ 快照轮询(其它)+ 递归展开
│   ├── scanstate/         断点续扫的记录格式(前向游标 + CRC 尾部)
│   ├── status/            状态文件(原子替换 JSON)
│   ├── logx/              日志(单 writer + 轮转 + 按 key 节流)
│   ├── guard/             nice / io_prio 降优先级(拒绝时只警告)
│   └── event/             统一事件模型(Op 位掩码)
├── .github/workflows/     CI:vet + gofmt + 测试 + e2e,打 tag 时自动发 release
├── deploy/                systemd unit、tmpfiles.d、sysctl 调优
├── examples/              带注释的完整配置模板
├── scripts/               dist.sh(交叉编译)、e2e.sh(端到端)、smoke.sh(目标机冒烟)
├── docs/                  DESIGN.md(设计与取舍)、lsyncd-analysis.md(原版逐行精读)、issue-matrix.md(733 条 issue 与 PR 逐条回归)
├── dist/                  交叉编译产物 + SHA256SUMS(由 scripts/dist.sh 生成)
├── Makefile               build / test / vet / e2e / smoke / dist / clean
├── go.mod                 仅 module 与 go 版本,无任何第三方依赖
├── LICENSE                GPL-2.0(与上游 lsyncd 一致)
└── README.md
```

## 命令行

| 参数 | 说明 |
|---|---|
| `-config PATH` | TOML 配置文件（必填，除 `-version` / `-status`） |
| `-logfile PATH` | 覆盖配置里的 `log_file` |
| `-loglevel L` | `debug` \| `info` \| `warn` \| `error` \| `none` |
| `-pidfile PATH` | 覆盖 `pid_file` |
| `-statusfile PATH` | 覆盖 `status_file` |
| `-nodaemon` | 前台运行（日志默认写 stderr） |
| `-once` | 只做初始全量同步，完成后退出 |
| `-status PATH` | 打印状态文件并退出 |
| `-check` | 校验配置（含源目录存在性）后退出 |
| `-watch-interval D` | 可移植轮询后端的扫描间隔（如 `30s`） |
| `-full-resync D` | 周期性全量重同步间隔；`-1`（默认）表示沿用配置，不覆盖 |
| `-version` | 打印版本 |

### 与同机其他服务共处

gsyncd 铺一次镜像会连续几小时吃满所有核与磁盘。如果这台机器还要跑 java / mysql / redis，把优先级降下来比调吞吐参数有用得多：

```toml
[global]
nice    = 10      # CPU:每次调度都让给同机的业务进程
io_prio = "idle"  # 磁盘:内核优先把带宽给 CFQ/BFQ 里的其他任务
```

两者都是 **best effort**：内核或容器拒绝时只记一条 `cannot lower priority (...)` 警告，同步照常进行 —— 没降成优先级的镜像仍然是能用的镜像。Windows 上不支持，会明确报错而不是假装成功。

nice 值是**逐个线程**设的（`setpriority` 在 Linux 上只影响调用线程，而拷贝 worker 跑在别的线程上），通过扫 `/proc/self/task` 完成，之后新建的线程继承父线程的 nice。IO 优先级同理。用 systemd 托管时也可以用 `Nice=` / `IOSchedulingClass=` 达到同样效果，两者不冲突（配置里的值会覆盖 unit 里的）。

信号：`SIGINT` / `SIGTERM` 优雅退出（退出前冲刷待处理事件，最多等 30s）；`SIGHUP` 由进程管理器重启即可。

默认会自己转到后台（重新执行自身 + `setsid`），父进程打印子进程 pid 后退出。用 systemd / supervisord 托管时加 `-nodaemon`（或 `nodaemon = true`），否则会得到「unit 是 active 但守护进程已经死了」这种典型故障。Windows 没有 `fork`/`setsid`，该平台不做后台化，请用服务或计划任务。

## 配置参考

### `[global]`

| 键 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `log_file` | string | `""`(stderr) | 日志文件 |
| `log_level` | string | `info` | 同上，另接受 lsyncd 的别名 `normal` / `scarce` / `all` |
| `log_max_size` | int | `104857600`(100MB) | 单个日志文件上限，超出轮转 |
| `log_max_files` | int | `5` | 保留的历史日志个数（`app.log.1` … `app.log.N`） |
| `pid_file` | string | `""` | 退出时自动删除 |
| `status_file` | string | `""` | JSON 状态文件，原子替换写入 |
| `status_interval` | duration | `10s` | 状态刷新间隔 |
| `max_processes` | int | `8` | 全局并发传输闸（跨所有 sync 共享） |
| `max_pending` | int | `500000` | 单个 sync 的聚合窗口上限；达到上限立即提前发送（不丢事件，也不触发全量） |
| `watch_interval` | duration | `30s` | 非 Linux 平台的轮询间隔（Linux 走 inotify，忽略此项） |
| `full_resync` | duration | `0` | 周期性全量重同步间隔。`0` 不是「关闭」：所有目录都装上 watch 时不做周期全量，有目录装不上时自动改成每小时一次。写明确值则一律按它跑 |
| `nodaemon` | bool | `false` | 前台运行。由 systemd 等托管时**必须**打开 |
| `nice` | int | `0`（不动） | CPU 调度优先级，-20（最优）…19（最差）。设为 10 让 gsyncd 排在同机 java/mysql/redis 之后 |
| `io_prio` | string | `""`（不动） | IO 调度类：`idle` \| `best-effort` \| `realtime`，可带级别如 `best-effort:7`。`idle` 让内核先把磁盘带宽给别人 |
| `memory_limit` | int \| string | `0`（不设） | 内存软上限，支持 `k`/`m`/`g`/`t` 后缀（如 `"1g"`）。交给 Go 运行时的 `debug.SetMemoryLimit`：接近上限时更积极地 GC 并把空闲页还给内核 |

### `[[sync]]`

| 键 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `name` | string | 源目录 basename | 日志与状态里的标识 |
| `source` | string | — | 被监视的本地目录 |
| `target` | string | — | 目标目录；`rsyncssh` 模式下只写 `host` 或 `user@host`，路径另见 `target_dir` |
| `target_dir` | string | 源目录的绝对路径 | 仅 `rsyncssh`：远端目标目录。省略时镜像到远端**同名绝对路径**（如 `/srv/data`），这通常不是你想要的，显式写出来 |
| `mode` | string | `direct` | `direct` \| `rsync` \| `rsyncssh` |
| `delay` | duration | `15s` | 事件静默多久后开始传输（整数按秒解释） |
| `max_hold`（也接受 `maxhold`） | duration | `1h` | 树一直不静默时（有文件被持续写）最多等这么久就强制刷一次。判的是**窗口里最老的那条变更**，不是逐路径判 —— 所以一个长驻写入方会把同窗口的其他文件一起按这个节奏刷出去（内容正确，只是批次更碎）。0 = 用默认 1h；负值 = 关闭上限，恢复「等到静默才拷」。见下文「文件一直开着写」 |
| `close_write` | bool | `false` | 等到 writer 关闭文件再同步（issue #458）。默认关闭：关闭时「事件静默 `delay` 秒」就算写完，写入期间目标端是半截快照但 close 后会自愈；打开后判定改为「文件已关闭」，半截快照基本消除，代价是从不关闭的文件只能靠 `max_hold` 收敛。**必须保留 `max_hold`**（关掉会被拒绝）。见下文「等到关闭再同步」 |
| `delete` | bool \| string | `true` | `true`/`false`，或 `"startup"`（只在首次同步清一次）/ `"running"`（首次不清，之后照删） |
| `excludes` | string[] | `[]` | 排除模式，见下 |
| `rsync_extra` | string[] | `[]` | 追加给 rsync 的参数 |
| `max_processes` | int | 0（用全局） | 该 sync 自己的并发上限；防止一个繁忙 sync 占满全局名额 |
| `workers` | int | 0(=`min(CPU,16)`) | `direct` 模式并行拷贝的 goroutine 数。51 万目录实测 1 个比 2 个慢约 1.8 倍，2–4 个即到平台期，默认值已落在平台期内 |
| `prefetch` | int | 0(=`min(CPU,16)`) | `direct` 模式初始遍历时**预读目录列表**的 goroutine 数。冷缓存下串行遍历只有一次元数据请求在飞，磁盘队列是空的：21 万目录实测 126s（串行）vs 15.9s（16 路）。**不改变访问顺序**，所以检查点仍然有效；负数关闭预读，退回 `filepath.Walk` |
| `batch_limit` | int | `10000` | 单次 rsync 调用的最大路径数 |
| `fsync` | bool | `false` | `direct` 模式下每个文件在 `rename` 前都 `fsync`。默认关闭：8 MiB 以上的文件无论如何都会刷盘，而对百万小文件逐个 `fsync` 比拷贝本身还慢 |
| `checkpoint` | string | `""`（关闭） | `direct` 模式的断点续扫记录文件。`"auto"` 落在 `status_file`/`log_file`/`pid_file` 同目录（都没有则 `$TMPDIR`），也可以写绝对路径。见下文「断点续扫」 |

`[sync.ssh]` 子表（仅 `rsyncssh`）：`binary`、`port`、`identity_file`、`options`（map）。

duration 接受 `15`（=15s）、`"500ms"`、`"2m"`、`"1h30m"`；键名同时接受 `snake_case` 与 `camelCase`；未知键会直接报错（避免拼写错误静默失效）。
`memory_limit` 是**软上限**，不是墙：它交给 Go 运行时的 `debug.SetMemoryLimit`，接近上限时运行时会更积极地 GC 并把空闲页还给内核，但不会因为超过它而杀掉进程 —— 真到那一步只有内核的 OOM killer 会动手。它的用途是让 gsyncd 与同机服务共处：设成机器可用内存的一小部分，gsyncd 就会在那个位置附近自己收手，而不是一路涨到被 OOM 挑中。注意它限制的是 Go 堆，内核里那张 watch 表不在其中（见 `deploy/99-gsyncd.conf`）。

配置是内置解析器读的 TOML 子集：`[table]` / `[[table]]` / `[table.sub]`、基本类型、数组（**可以跨行**，行尾可带注释）、`'单引号'` 与 `"双引号"` 字符串。不支持多行字符串、日期字面量、dotted key —— 需要时直接写等价的 `[sync.ssh]` 形式。

`direct` / `rsync` 的 `target` 不能放在 `source` 里面（每次拷贝都会变成新的源文件，自我复制直到磁盘满），`source` 也不能放在 `target` 里面（`delete` 会把它删掉）。两种情况在加载配置时直接报错。

### 排除模式

与 rsync 风格一致：

- `*.tmp` —— 任意层级的 basename 匹配
- `/build` —— 仅匹配源根下的 `build`（前导 `/` 锚定）
- `cache/` —— 匹配目录 `cache` **及其全部内容**（尾随 `/`；对文件只匹配真祖先，所以叫 `tmp` 的普通文件不会被 `tmp/` 误伤）
- `**/node_modules/**` —— 跨段通配
- `tmp[0-9]` —— 字符类，支持 `[a-z]` 与 `[!abc]`
- `!keep.tmp` —— 取反（后出现的规则优先）

任何匹配到目录的模式都会覆盖该目录下的全部内容 —— 与 rsync 一致，它从不进入被排除的目录。原版只匹配条目本身，于是在被排除目录里新建的文件会重新出现在目标端。

同一份列表会在三处生效：watcher 不进入被排除的目录、事件不会进入批次、**首次全量拷贝也会跳过它们**（rsync 后端转成 `--exclude=`/`--include=`，direct 后端在遍历时直接 `SkipDir`）。原版只在事件侧和 rsync 参数侧过滤，启动扫描阶段的行为要看你额外传了什么参数。rename 在目标端找不到旧名字时会回退成整棵拷贝，这条回退路径同样过滤（它会把子树内相对路径重定基到同步根再匹配）。

模式永远用 `/` 分隔；Windows 上收到的 `\` 会在匹配入口归一化。判定过程不分配内存、不取锁：8 条单段规则（如 `*.tmp`、`node_modules`）判一条路径约 508ns，并发下约 81ns。这里原本有一张「路径 → 是否排除」的缓存表，实测被删掉了 —— 值得优化的大树上几乎每条路径都唯一，缓存命中率为零，20 万条上限还要常驻 13MiB。

## 状态文件

```json
{
  "version": 1,
  "pid": 24464,
  "started": "2026-11-05T11:36:00+08:00",
  "updated": "2026-11-05T11:36:11+08:00",
  "uptime_seconds": 11,
  "watched_dirs": 3,
  "unwatched_dirs": 0,
  "phase": "running",
  "rss_kb": 129116, "heap_kb": 92582,
  "lost_roots": ["/srv/data"],
  "syncs": [
    {
      "name": "src", "source": "...", "target": "...", "backend": "direct",
      "pending": 0, "batches": 2, "events": 6, "filtered": 0, "applied": 6,
      "files": 6, "deletes": 2, "errors": 0, "retries": 0, "full_syncs": 0,
      "overflows": 0, "root_gone": 1, "root_lost": true, "peak_pending": 4,
      "starting": true
    }
  ]
}
```

`phase` 是进程当前阶段：`watching`（还在装内核 watch）、`initial`（首次全量在跑）、`running`（事件循环接管）。前十 TB 的树上前两个阶段都以小时计，而所有传输计数在这期间全是 0 —— 没有这个字段，「正在装 watch」「正在拷」「卡死」三者长得一模一样。

`starting` 为 true 表示这个 sync 的首次全量还在跑 —— 十 TB 的树这一步要几小时，期间除 `watched_dirs` 外的计数都停在 0，没有这个标志就分不清「正在拷」和「卡死」。

`applied` 是「应用了多少条折叠后的变更」，`files` 是「真正写到目标端的文件数」，两者在事件洪峰里会差很远（一万次写同一文件 = 1 条 applied、1 个 file）；`deletes` 是目标端真正删掉的条目数，批量删除时它才是进度条。

三个字段是运维要盯的：`unwatched_dirs`（内核拒绝的目录数）、`lost_roots`（当前 watch 已失效的源根列表）、`root_lost`（该 sync 现在是否瞎的）。三者任一非空/非零，镜像就已经在静默漂移 —— 进程照样健康、计数照样正常，只在镜像里体现为差异。`root_gone` 是「根目录 watch 被重建过几次」的累计计数，配合 `re-attached ... forcing a full resync` 日志行看。

`rss_kb` 是整进程常驻内存（读 `/proc/self/statm`，与 `ps -o rss` 逐 kB 一致）；`heap_kb` 是 Go 堆上活对象（`runtime.MemStats.HeapAlloc`，取它而不是 `Sys` —— `Sys` 含已保留但从未触碰的地址空间，会把占用夸大一个数量级）。两者都是 0 表示读不到 `/proc`。想看有没有慢泄漏，盯 `rss_kb` 在 `watched_dirs` 稳定之后是否还单调上涨；大规模实测里它在 110~131MB 之间波动而不增长。

## 后端

| mode | 需要什么 | 行为 |
|---|---|---|
| `direct`（默认） | 无 | 进程内拷贝：写 `.gsyncd-tmp-*` → `fsync` → `rename`；删除多余项；不跟随符号链接 |
| `rsync` | `rsync` | 变更列表通过 `--files-from=- --from0` 从 stdin 传入（不受 `ARG_MAX` 限制） |
| `rsyncssh` | `rsync` + `ssh` | 同上，目标为 `host:path`，附带 `-e ssh` 参数 |

`rsync` / `rsyncssh` 都带 `--partial --partial-dir=.rsync-partial`，大文件中断后可续传。

**`--delete` 只出现在首次全树传输里。** 增量批次只携带变更路径，若再叠加 `--delete`，rsync 会把目标端其余所有内容判为「源端不存在」而删除 —— 在 10T 级镜像上等于清空目标。运行期删除改为精确路径的删除（本地直接 unlink，远端用 `--delete-missing-args --force`），用户写在 `rsync_extra` 里的 `--delete*` 也会被剥离（需要镜像语义请用 `delete = true`）。

`--force` 不是可选项：rsync 3.1.2 实测 **`--delete-missing-args` 遇到非空目录会打印 `cannot delete non-empty directory` 却仍然返回 0**，于是「重命名一个目录」在目标端留下整棵旧子树，镜像静默漂移、日志里一个错误都没有。加上 `--force` 才会连同内容一起删除，与 `direct` 后端 `os.RemoveAll` 的行为对齐。

同理，**rename 必须在目标端删掉旧名字**：rsync 没有客户端 rename，`mv a b` 只传 `b` 的话 `a` 会永久留在目标端，每次改名都让镜像膨胀一点。两种模式都会显式删除旧路径。

## 文件一直开着写

一个文件被进程长时间持有并持续追加时（append-only 日志、数据库 WAL、正在被别的工具灌数据的大文件），它在 `close` 之前不会有 `IN_CLOSE_WRITE`。原版只订阅了 `IN_CLOSE_WRITE`，于是这类文件在整个写入期间**一条事件都没有**，镜像停在第几 MB，直到 writer 关闭或轮转 —— 上游 issue #77、#458 说的就是这件事。

gsyncd 的 mask 里多订阅了 `IN_MODIFY`，但**不是**每写一次就拷一次：

- 连续 `IN_MODIFY` 在聚合器里被折叠成同一条 pending change，每个事件只花一次 map 更新；
- 传输仍然等 `delay` 静默，所以一个正常写完就关的文件仍然只拷一次；
- 如果它**永远不静默**（每秒都在写），`max_hold` 到点就强制拷一次，再继续等静默。

这条路径保证「写入期间镜像也在前进」，代价是每份快照都取自写入中途。要的是完整快照而不是持续追赶，看下一节。

`max_hold` 默认 `1h`。选这个值的理由：它要长到几 GB 的拷贝、数据库 checkpoint 这类正常写入突发不会变成反复全拷，又要短到一个班次内能把 append-only 日志追上；一小时两头都满足。真机上用 `max_hold = 5` 复测（每 1 MiB/秒追加）：目标端从冻结变成每约 8 秒增长一次，结束时源与目标的 `sha256` 完全一致。

代价要说清：这类文件在写入期间镜像总是落后一点（最坏 `max_hold + delay + 一次拷贝时间`），而且每次强制拷贝都从文件头开始读 —— 对 10 GB 级、持续被写的文件，这是可观的读放大。要更贴近，就把 `max_hold` 调小；要更省 IO，就调大，或者干脆用 `excludes` 把这类文件排除掉，让数据库自己的备份机制去处理。

## 等到关闭再同步（`close_write`）

上一节的做法让「一直被写」的文件能被周期性追上，但每一份快照都取自写入中途 —— 目标端那份文件在 writer 关闭前总是半截。对 append-only 日志这没问题，对数据库数据文件、虚拟机镜像、被 mmap 的索引就不是：目标端看起来完整，实际是撕裂的快照，而下次同步又会因为 size+mtime 与源一致而跳过它。

`close_write = true` 把判定从「事件静默了」换成「writer 关闭了」。它**默认关闭**，而且必须保留 `max_hold`：

```toml
[[sync]]
name        = "pg"
source      = "/srv/pg"
target      = "/backup/pg"
delay       = 5
max_hold    = 300        # 必须留着：从不关闭的文件靠它兜底
close_write = true
```

为什么不能默认开：一个文件被打开一次然后永远不关（数据库 datafile、mmap 文件、长驻进程持有的句柄）在事件层面与「正在被写」完全无法区分，两者都是「还没收到 close」。默认开会让这类文件**永远不同步**，而且 `pending=0`、`errors=0`、`phase=running` 全绿、一条日志都没有 —— 全绿但数据错，比报错危险得多。所以这个开关必须由知道那棵树里文件会被关闭的人来打开，而 `max_hold` 是那个保证「无论如何最终会拷一次」的兜底：`close_write = true` 与 `max_hold < 0` 的组合在配置校验时直接拒绝。

三种组合的实际风险：

| 配置 | 半截文件 | 陈旧上限 | 永久不同步 |
|---|---|---|---|
| 默认（`close_write` 关） | 可能（写入期间每次快照都是半截） | `delay` | 不会 |
| `close_write = true` + `max_hold` | 基本消除（仅 `max_hold` 强刷那一次是半截） | `max_hold` | 不会 |
| `close_write = true` 且关 `max_hold` | 消除 | 无 | **会**（被拒绝） |

真机实测（8 核 / 62G，目标端同盘）。写者每秒 append 256KiB 共 20 秒、写完再保持 fd 打开 20 秒才 close，`delay = 2`：开了 `close_write` 时目标端在 40 秒内**连文件都不存在**（`open=1 pending=1 files=0`），close 后 4 秒落盘 5,242,880 字节；默认配置在 `t+24s` 就已经躺着一份快照。两者最终 sha256 都一致。

更直接的对照：写完 2MiB 后暂停 12 秒（fd 仍打开），暂停期超过 `delay`。默认配置此时目标端**已经躺着半截的 2MiB**，读取方看得到；开了 `close_write` 则目标端还没有这个文件，close 后一次写入完整的 4MiB。所以半截快照不是理论风险，它确实会落到目标端。

`max_hold` 是唯一逃生口：同样 20 秒写入 + 20 秒不关闭，把 `max_hold` 设成 8 秒，日志里会出现四次 `applied`（每 8 秒被强刷一次），close 后最终一致 —— 永不关闭的写者也不会永久不同步。

语义细节：

- `delay` 仍然表示「安静这么久」，close 只是把计时起点挪到关闭那一刻，不是立即刷。所以一个写完就关的文件行为与不开这个开关时一样（仍然只拷一次）。
- 只统计**真正被写过的**文件。单独的 `create` 不算打开 —— 它也可能是 `ln -s`、fifo、设备节点或 rename 落位，这些永远不会收到 close，算作打开会让一条 `ln -s` 把整窗卡到 `max_hold`。目录同理。
- 重命名不关闭文件：标志跟着条目走。传输失败回填窗口时也保留原状态，不会把已经到达的 close 撤销。
- 丢事件（overflow）与撞上 `max_hold` 时照常立即刷新 —— 那时镜像已知不正确，再等 close 只会让错误状态持续更久。

代价：窗口是一起刷的，所以只要还有一个文件开着，同批次里其他已关闭的文件也要等到它关闭或者 `max_hold` 到点。默认 `max_hold = 1h` 下这意味着一个长驻写入方能把同窗口的批次节奏拖到一小时一次（内容仍然正确）。**要避免就把常写目录拆成独立的 `[[sync]]`**，给它自己的小 `max_hold`：

```toml
[[sync]]
name        = "pg"
source      = "/srv/pg"
target      = "/backup/pg"
delay       = 30
max_hold    = 3600
close_write = true

[[sync]]
name        = "pg-wal"
source      = "/srv/pg/pg_wal"
target      = "/backup/pg/pg_wal"
delay       = 2
max_hold    = 60
close_write = true
```

引擎按最长前缀路由，嵌套 root 下事件只进最长前缀那条，所以 `/srv/pg/pg_wal` 的写入不会影响 `/srv/pg` 其余部分的节奏。

## 断点续扫（`checkpoint`）

`direct` 后端可以给每个 sync 配一个检查点文件，把「这些目录已经和目标端一致」这件事持久化。重启时不再重新证明一遍整棵树，只验证一遍：

```toml
[[sync]]
name = "data"
mode = "direct"
source = "/srv/data"
target = "/backup/data"
checkpoint = "auto"          # 或 "/var/lib/gsyncd/data.scan"
```

**它省掉的是什么。** 不带检查点时，每次启动都要对源端每一个目录做 `readdir`、对目标端每一个目录做 `readdir`、对每个文件 `stat` 一次目标端。实测重扫已同步好的 100 万文件是 2,336 文件/秒（只比 `size`+`mtime`），外推 10T 的 5000 万文件 ≈ 5.9 小时纯扫描。带检查点时，源端仍然要遍历（停机期间的改动只有 `stat` 看得见），但**内容未变的目录完全不碰目标端**：不 readdir、不逐文件 stat、不 prune。源端一次遍历 + 每个目录一次 `stat` 目标目录。420GB / 220,101 目录的真机实测：重启到 `phase=running` **20.1s**，其中扫描比对 14.4s，这一轮 `files=0`。

**它凭什么敢跳过。** 一个目录被跳过，必须同时满足三条：

1. 记录里的源目录 mtime、子项数量、子项摘要，与本次 `readdir` 算出来的三者完全一致；
2. 目标目录存在，且它的 mtime 等于记录的源目录 mtime；
3. 检查点文件的指纹匹配（源路径、目标路径、exclude、模式都没变）。

第 2 条是关键：每次同步完一个目录，gsyncd 把**目标目录的 mtime 设成源目录的 mtime**（和 `rsync -a` 一样）。所以任何事后动过镜像的操作 —— 手工删文件、从旧备份恢复、目标盘崩过 —— 都会改变目标目录 mtime，让记录当场失效。摘要里子目录只折名字、不折子目录的 mtime，否则一次叶子写入会让整条祖先链失效，持续写入的树上永远命中不了。

**它会怎么坏，以及坏了之后怎么办。** 检查点是加速器，不是证据。文件缺失、被截断、CRC 不符、指纹不符、路径不可写、目录被当成文件 —— 每一种都退化成「没有检查点」，多走一遍全量，绝不会少走。目录在拷完**之后**才写记录，所以被杀掉的进程留下的尾部最多少记一批，不会记进没做完的活。

```
$ grep "scan state:" /var/log/gsyncd.log
scan state: 1000001 directories verified, 1000001 reused, 0 recorded, 0 dropped
```

`verified` 是本次走到的目录数，`reused` 是其中直接跳过的，`dropped` 是因为记录缓冲满而没写下的（下次多验证几个，安全方向）。可以随时删掉检查点文件，下一次启动就是一次完整核对。

> 只有 `direct` 模式有检查点。`rsync` / `rsyncssh` 的跳过判断在 rsync 自己手里（`--size-only` 或默认的 size+mtime），gsyncd 不重复做。

## 从 lsyncd 迁移

| lsyncd (`lsyncd.conf.lua`) | gsyncd (`gsyncd.toml`) |
|---|---|
| `settings.logfile` | `[global] log_file` |
| `settings.log = "normal"` | `[global] log_level = "info"` |
| `settings.statusFile` | `[global] status_file` |
| `settings.maxProcesses` | `[global] max_processes` |
| `settings.nodaemon = true` | `[global] nodaemon = true` |
| `sync{default.rsync, source=..., target=..., delay=15}` | `[[sync]] mode="rsync" source=... target=... delay=15` |
| `default.direct` / `default.rsyncssh` | `mode = "direct"` / `"rsyncssh"` |
| `delete = "startup"` | `delete = "startup"` |
| `exclude = { "*.tmp" }` | `excludes = ["*.tmp"]` |
| `rsyncOpts = { "-z" }` | `rsync_extra = ["-z"]` |
| `ssh = { port = 2222 }` | `[sync.ssh] port = 2222` |

配置语言从 Lua 换成 TOML，正是为了去掉「必须装 Lua」这个依赖：原版把用户配置当 Lua 代码执行，`gsyncd` 只解析数据。

## 生产部署

仓库里已经带了整套可直接改路径就用的文件：

| 文件 | 用途 |
|---|---|
| `examples/gsyncd.example.toml` | 全参数带注释的配置模板（含 direct / rsyncssh / rsync 三种场景） |
| `deploy/gsyncd.service` | systemd unit（已处理 `-nodaemon`、排空超时、重启策略） |
| `deploy/gsyncd.tmpfiles.conf` | `/var/lib/gsyncd`、`/var/log/gsyncd` 目录 |
| `deploy/99-gsyncd.conf` | sysctl 调优（inotify 上限、脏页回写、句柄），面向 10T 级 |
| `scripts/smoke.sh` | 拿到二进制后先跑的冒烟脚本：在本机建临时目录验证读→写→实时同步→状态全通；含连续建 5 个目录并立刻写入、以及两级嵌套新建目录的竞态断言 |

```bash
# 0. 先在本机冒烟:证明这个 arch/kernel 上二进制真的能跑、能拷、能跟上事件
#    (在 /usr/local/bin 装好后、指向真实数据之前做,几十秒出结果)
bash scripts/smoke.sh ./dist/gsyncd-linux-amd64    # 期望输出 SMOKE OK

# 1. 二进制(静态、零依赖,可直接 scp 到目标机)
install -m 0755 dist/gsyncd-linux-amd64 /usr/local/bin/gsyncd

# 2. 配置
install -d /etc/gsyncd
install -m 0600 examples/gsyncd.example.toml /etc/gsyncd/gsyncd.toml
$EDITOR /etc/gsyncd/gsyncd.toml          # 改 source / target / excludes
/usr/local/bin/gsyncd -config /etc/gsyncd/gsyncd.toml -check

# 3. 内核参数(同步 10T 时建议全部启用,详见文件内注释)
install -m 0644 deploy/99-gsyncd.conf /etc/sysctl.d/99-gsyncd.conf
sysctl --system

# 4. systemd
install -m 0644 deploy/gsyncd.service /etc/systemd/system/gsyncd.service
systemctl daemon-reload
systemctl enable --now gsyncd
systemctl status gsyncd
journalctl -u gsyncd -f
```

**启动顺序要点**：`-check` 会真的去 stat 源目录，所以目标盘没挂上时它会失败（这是好事 —— 避免 `delete = true` 对着空挂载点把备份清空）。独立数据盘请打开 unit 里注释掉的 `RequiresMountsFor=`。

### 10T 规模要调的内核参数

完整理由写在 `deploy/99-gsyncd.conf` 的注释里，这里只列结论：

| 参数 | 建议 | 为什么 |
|---|---|---|
| `fs.inotify.max_user_watches` | 目录数 × 1.2 向上取整（`find /srv/data -type d \| wc -l`） | 每个 watch 约 100B **不可换出**内核内存（实测 97~115B，见 DESIGN §6.2）；默认 8192 / Debian 524288 在几十万目录时直接 ENOSPC |
| `fs.inotify.max_queued_events` | `262144` | 默认 16384；`rm -rf` 二十万文件会在几秒内产生二十万事件，溢出后只能全量重传（10T 全量是小时级） |
| `vm.dirty_background_ratio` | `5` | 默认 20% 在大内存机上会攒几十 GB 脏页再集中回写，表现为「拷贝突然停几十秒」 |
| `vm.dirty_ratio` | `15` | 同上，硬上限 |
| `vm.vfs_cache_pressure` | `50` | 镜像负载反复 stat 同一批路径，让 dentry/inode 缓存多留一会儿 |
| `fs.file-max` / `fs.nr_open` | `2097152` | 配合 `LimitNOFILE`；rsyncssh 每批会开 ssh 连接 |

网络相关的 `tcp_rmem` / `tcp_wmem` / BBR 只在 `rsyncssh` 模式有意义，unit 文件里已注释好，按 RTT 算窗口（1Gbps × 30ms ≈ 3.75MB）。

**不建议**动 `vm.overcommit_memory`：它和同步速度无关，只是关掉内核对 mmap/brk 的额度检查 —— 任何进程都能申请到远超物理内存的地址空间，代价是把「申请时失败」推迟成「真正写入时被 OOM kill」。gsyncd 自己不需要它（Go 堆有 `memory_limit` 兜着）。反过来要注意：机器上已经设了 `overcommit_memory = 2`（严格模式）时，Go 运行时预留的虚拟地址空间可能让 gsyncd 起不来，那种情况下才需要放宽 `overcommit_ratio` 或改成 `1`。

### 首次同步 10T 时的实操建议

1. **先跑 `-once` 把全量铺完，再开守护进程。** 直接启动守护进程也能工作（它在 `Startup` 里做全量），但 `-once` 可以前台观察进度、退出码明确，也方便你决定失败后是否要重试。
2. **两个并发旋钮，别混。** `workers` 是 direct 后端的并行拷贝 goroutine 数（默认 `min(CPU,16)`）：51 万目录的实测里 1 个 worker 比 2 个慢 1.8 倍，超过 4 个就没收益了。`max_processes` 限的是同时在飞的传输批次数，是**限速**旋钮不是加速旋钮 —— 对 rsync 后端就是并发子进程数，每个子进程都要 fork 并自己走一遍目录树，机械盘阵列上 2–4 往往比 8 更快（原版 issue #75 就是启动时无限制并发把磁盘打满）；direct 后端每个 sync 本来就一次只跑一批，所以它在那里基本不生效。
3. **`delay` 在初次全量阶段不起作用**（全量是同步做的），它只影响事件聚合窗口。批量导入数据时给 `excludes` 加上临时目录、日志目录，能少 watch 一大片。
4. **目标端不要放在源端路径下**，配置加载时就会报错；同理 `source` 和 `target` 也别设成同一个目录。
5. **看进度看状态文件**：`gsyncd -status /var/lib/gsyncd/status.json` 里的 `pending` / `events` 是引擎视角的积压量；`peak_pending` 是历史峰值，持续贴着 `max_pending` 说明聚合窗口跟不上事件产生速度。

### 实测数据（8 核 / 62G / XFS）

| 规模 | 指标 |
|---|---|
| 12 万文件 / 4 万目录 | 初始全量 8s；2 万文件 burst 18s 收敛；删 20 棵子树 7s 收敛；重命名目录 8s 收敛；2000 个等长改写 6s 收敛，差异全部为 0 |
| 51 万文件 / 51 万目录 / 2G | 装 watch 10s；初始全量 2m21s（`workers = 1`）到 1m18s（`workers = 2`）；RSS 142–193MB（整进程约 280–380B/目录，其中索引本身约 60B/目录；内核另约 100B/目录不可换出） |
| 同上，同一棵树交替对照 `cp -a` | 10 万文件级：gsyncd 快 26%–55%；51 万目录级：`cp -a` 快 10%–25%（单线程顺序写更贴合这个虚拟盘的元数据路径） |
| 后端吞吐（直接灌 4 万变更，轮换测量） | 2–4 个 worker 即平台期 1.3–1.7 万 changes/s；1 个 worker 约 0.8 万（比平台期慢 1.6–2.0 倍），16/32 无进一步收益 |
| 链路完全断开 60s | 0.4% CPU（原版 issue #250 是 100% 忙等），日志 11 行 |
| 链路抖动 90s 后恢复 | 断链期 0.4% CPU；400 条事件一条不丢；恢复后自动收敛到 500 文件 |
| 内核队列溢出（压到 512,20 万文件突发） | 75s 收敛，源/目标各 200001 文件；`full=2`；RSS 83MB；日志 11 行 |
| 100 万目录装 watch（边 `readdir` 边装）/ 100 万文件首拷 | 92.9s（10,780 目录/秒，RSS 382MB）/ 13m40s（1,219 文件/秒）。纯装表更快：热缓存 7.4 万/秒、冷缓存 1.8 万/秒 |
| watch 索引内存随目录数（`TestMemoryPerDirectory`） | 100 万 **61B/目录**、2000 万 **47B/目录**（heap 880MB→1822MB，103s）、5000 万 **42B/目录**（heap 2320MB→4420MB，274s）；随规模**下降**，没有拐点 |
| 聚合窗口内存随路径数（`TestMillionPathsMemory`） | 50 万 **243B/路径**、100 万 **235B/路径**、200 万 **234B/路径**、2000 万 **215B/路径**、5000 万 **199B/路径**（5000 万条 3m40s）；`max_pending=500000` 实际约 120MB |
| 420GB / 210 万文件真机全量后重启（开 `checkpoint`） | 装 watch 3s；扫描比对 **14.4s**（176,000 目录全部 reused）；到 `phase=running` **20.1s**；这一轮 `files=0 / deletes=0 / events=0`，一个字节没重传 |
| 200KB 文件纯拷贝 | 3,254 文件/秒 = 651 MB/秒 |
| 持续写入 505 / 1,861 文件/秒 | 8.5% / 35% CPU；前者零丢失，后者积压后在写完后 3 分钟内追平（磁盘写不完，非代码瓶颈） |
| 裸盘 `dd conv=fdatasync` / 写放大 | 132 MB/秒 / 2.07 倍 |
| 同一棵树上 `cp -a` 对照（200KB × 4 万） | 138 文件/秒（26 MB/秒），gsyncd 快 23 倍 |
| 批量删除（聚合层折叠，真机） | 1 万 / 10 万 / 20 万 / 50 万窗口文件（对应 2 千 / 2 万 / 4 万 / 10 万个待删目录）分别 4.09ms / 60.4ms / 113.8ms / 292.6ms，每目录 2–3µs 不随规模上升；同场景修复前 58.4s 与 >120s 超时 |
| 批量删除专项（源+目标都在 tmpfs，21 万目录 / 20 万文件全删） | 62 万事件 → **1 个批次 / 200 条变更**，16s 收敛烧 **9.4s CPU**（47µs/目录），`peak_pending` 5,206，RSS 峰值 127MB，`overflow / full_syncs / errors` 全 0 |
| 批量删除专项（源+目标都在 tmpfs，**500 万文件** / 1 万目录，全删） | **502 万事件 → 1 个批次 / 20 条变更**，30s 收敛烧 **5,273 ticks**（约 10µs/事件），`peak_pending` 7,499，RSS 峰值 **40MB**，`overflow / full_syncs / errors` 全 0 |
| 500 万文件初始全量（tmpfs，纯元数据） | 475s，`VmHWM` **40MB**（修复预读失控前是 1,077MB），CPU 773 秒 ≈ 1.6 个核 |
| 420GB / 210 万文件真机端到端（XFS） | 装 watch 421s；初始全量 **5h02m**，期间 RSS 88–156MB 不随字节数增长、CPU 10–20% 单核；改 1000 文件 10s 收敛；删 20 个顶层（42 万文件）182s 收敛烧 5.2s CPU，`peak_pending` 1,145，`errors / overflows` 全 0 |
| 源根目录 `rm -rf` 后原样重建（2 万目录 / 10 万文件） | 内核通知一条没到（`IN_Q_OVERFLOW` 抢在 `IN_IGNORED` 之前），靠 inode 身份校验在 20s 内重挂并全量收敛；新建文件 5s、改文件 5s、删文件 10s 均正确镜像 |

**10T 能不能扛住**：见 [DESIGN.md §6.2](docs/DESIGN.md)。结论是「gsyncd 不是瓶颈，磁盘和每目录文件数才是」——10T / 200KB ≈ 5000 万文件，每目录 ≥10 个文件时可行（500 万目录 = 约 500MB 内核不可换出 + 约 210–305MB 用户态索引），首次全量 4.3–46 小时取决于目标盘带宽，持续写入上限约 670 文件/秒；开 `checkpoint` 后重启实测 20 秒回到 running、零字节重传（420GB / 220,101 目录），不配才是 5.9 小时。

**再往上一个量级**：100T ≈ 5 亿文件 / 5000 万目录，同一套算术仍然成立 —— 用户态索引约 2.1GB（实测 42B/目录）、内核不可换出约 5GB、装 watch 约 46 分钟（实测 1.8 万目录/秒），`max_user_watches` 要给到 5000 万。**1000T 则超出单机**：5 亿个 watch 约 50GB 不可换出，装 watch 要 7.7 小时，首次全量 17.8 天（651MB/秒）到 181 天（132MB/秒 + 写放大）。唯一现实路径是按**目录数**分片，每个 `[[sync]]` 只管一棵子树、单实例 `watched_dirs` ≤ 500 万；算术见 [DESIGN.md §6.2 结论五](docs/DESIGN.md)。

数据来源与测量方法见 [docs/DESIGN.md](docs/DESIGN.md) 的「真机压测数据」与「故障注入」两节，包括三个容易踩的测量陷阱（`diff -r` 在内容相同的合成数据上会误报一致；harness 自己的全树走查会吃掉一个核；共享主机上 `loadavg` 是全局的，只能成对测量）。

## 构建

```
$ CGO_ENABLED=0 go build -buildvcs=false -trimpath -ldflags "-s -w" -o gsyncd ./cmd/gsyncd
$ bash scripts/dist.sh  # 一次产出 linux/darwin/windows 六个目标到 dist/,附 SHA256SUMS
```

产物是静态链接的单文件（linux-amd64 约 2.8MB），可直接扔进 scratch / distroless 镜像。

## 测试

```
$ go test ./... -count=1     # 单元测试(Linux 上实测 237 个:215 个 Test + 22 个 Benchmark,含 inotify/guard 的真内核用例;
                             # 另有 4 个 //go:build !linux 的轮询后端用例,跨平台合计 241 个)
$ bash scripts/e2e.sh         # 端到端:启动全量 → 改文件 → 新增 → 新建子目录 → 删目录 → 重命名 → 状态
```

`e2e.sh` 会真的启动二进制、真的改文件系统，并在每一步之后断言目标端的内容 —— 包括「被排除的路径一次都没出现」「删掉的目录真的消失」「重命名后旧名字不残留、新名字带全部内容」。排除规则与 rename 这两块最容易在跨平台时静默失效，所以它们在脚本里是硬断言而不是日志观察。

详见 [docs/DESIGN.md](docs/DESIGN.md)：架构分层、与 lsyncd 的逐项差异、以及针对 10T–100T 规模的取舍。
原版 18090 行 C/Lua 的逐行阅读记录与缺陷清单见 [docs/lsyncd-analysis.md](docs/lsyncd-analysis.md)。
上游 733 条 issue / PR 逐条判定（哪条已修、哪条用配置就能满足、哪条确认不做）见 [docs/issue-matrix.md](docs/issue-matrix.md)。

也可以用 Makefile:

```
$ make build      # 本机二进制
$ make test vet   # 215 个 Test + 22 个 Benchmark(Linux),外加 go vet 与 gofmt 检查
$ make e2e smoke  # 端到端 + 冒烟
$ make dist       # 六个目标到 dist/
```

## 许可证

GPL-2.0-or-later，与上游 [lsyncd](https://github.com/lsyncd/lsyncd) 一致（见 [LICENSE](LICENSE)）。
gsyncd 是独立的 Go 实现，不含上游的 C / Lua 源码；沿用同一许可证是为了尊重它所继承的设计。
