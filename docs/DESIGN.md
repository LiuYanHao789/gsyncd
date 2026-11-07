# gsyncd 设计说明

本文说明三件事：**为什么要重写**、**新架构怎么分层**、**每一条已知缺陷/瓶颈在新实现里怎么被消掉**。
对原版的逐行精读见 [`docs/lsyncd-analysis.md`](lsyncd-analysis.md)（lsyncd 2.3.1 全仓库 18090 行的逐行阅读记录、缺陷清单与规模瓶颈分析）。上游 733 条 issue 的逐条判定见 [`docs/issue-matrix.md`](issue-matrix.md)。

---

## 1. 为什么重写

lsyncd 的架构本身是合理的：C 只负责「贴近系统」的部分（inotify、fork/exec、信号、日志、闹钟），业务逻辑放在 Lua。问题在于这套分工在今天付出了三个代价：

1. **部署依赖**：必须编译 C + 链接 liblua，运行期还依赖 rsync/ssh；交叉编译、嵌入式、容器里都是负担。
2. **单线程**：`masterloop` 是一个 `pselect` 循环，Lua 侧的过滤/聚合/决策全在这一条线程里，大事件洪峰时既不能吃多核，GC 也直接顶到吞吐上限。
3. **数据结构为大目录树设计不足**：delay 队列线性查找（O(n²)）、1000 条硬上限、watch 与路径映射全在内存、溢出即重启。

`gsyncd` 保留 lsyncd 的心智模型（`sync` + `delay` + `exclude` + `delete`），但把实现整个换掉。

---

## 2. 分层

```
cmd/gsyncd          CLI、信号、pidfile、-check / -status / -once
  ├ internal/config     TOML 解析(自写,零依赖)+ 校验
  ├ internal/engine     编排:每 sync 一个 aggregator + delay 定时器 + 传输闸
  │   ├ internal/aggregate  事件折叠(哈希表,每事件 O(1))
  │   ├ internal/filter     排除模式(编译一次,单遍匹配,不缓存结果)
  │   └ internal/sync       Backend 接口:direct / rsync / rsyncssh + 退避重试
  ├ internal/watcher    Backend 接口 + 递归层
  │   ├ inotify_linux.go    inotify_init1 + 变长解析 + cookie 配对 + MoveGrace
  │   └ poll_other.go       快照 diff(macOS/Windows/BSD)
  ├ internal/status     状态文件(原子替换 JSON)
  ├ internal/logx       日志(单 writer + 轮转 + 按 key 节流)
  └ internal/event      统一事件模型(Op 位掩码)
```

数据流：

```
kernel / 文件系统
      │  inotify 事件 / 快照 diff
      ▼
watcher.Tree ── 递归展开、cookie 配对、MoveGrace 降级、Overflow 透传
      │  <-chan event.Event(单一有界通道)
      ▼
engine.Root.Handle ── 按路径前缀路由到对应 sync,过 filter
      │
      ▼
aggregate.Aggregator ── 同路径合并;delete→create 变 write;create→delete 消失;
      │                  目录删除吸收子树;move 抹掉源路径
      │  delay 窗口到期(最后一次事件 + delay)
      ▼
sync.Backend.Apply(Batch) ── 进程闸(semaphore)+ 失败退避 + readd 重试
```

接收、折叠、传输是三组独立 goroutine；通道有界，天然形成背压。慢后端只会让聚合窗口变大，不会让进程 OOM。

---

## 3. 缺陷与瓶颈 → 新实现

### 3.1 确定性 bug

| 原版缺陷 | 位置 | 新实现 |
|---|---|---|
| `Queue.remove` 用 `nt.first` 当 `last`，中段删除后队列视图变空（issue #725） | `lsyncd/lsyncd.lua:606` | 整个 `Delay` / `Queue` / `Combiner` 被 `aggregate` 替代：变长切片 + `map[path]index`，删除用 swap-remove（与末尾交换后截断，`O(1)`），索引表同步修正 |
| `l_exec` 每次同步都 `l_stackdump` | `lsyncd.c:1223` | 不存在：没有 Lua 栈，也没有内嵌解释器 |
| `dump()` 在 Lua 5.5 报 `attempt to assign to const variable 'k'` | `lsyncd/lsyncd.lua:58-78` | 不存在 |
| jiffies 换算写成乘、`%Lf` 传指针 | `lsyncd.c` | `time.Time` + `time.Since`，单调时钟 |
| `l_free_port` 的 TOCTOU | `lsyncd.c` | 不存在（没有需要协商端口的子进程） |
| `masterloop` 吞掉 `pselect` 的 -1 | `lsyncd.c` | Go runtime 的 netpoller，无需手写 select |
| `nonobservances` 只比较栈顶（FIXME） | `lsyncd.c` | 不存在 |
| `main1` 错误路径 `exit(-1)` 不清理 | `lsyncd.c` | 所有资源用 `defer` 回收；pidfile 退出删除；错误逐层返回 |
| `fsevents.c` 复制粘贴的错误文案 + 缺分号 | `fsevents.c` | 该文件不被移植；macOS 走 `poll` 后端 |
| exclude 模式在 Lua 侧空转：`[a-z]` 被转义成字面量、`dir/` 拼出 `dir//` 永不命中 | `lsyncd/lsyncd.lua:2161-2193` / `:2289-2319` | `filter` 自己编译模式：字符类、`**`、锚定与取反都在 Go 侧实现，**且与转给 rsync 的 `--exclude=` 用同一份规则** |

### 3.2 性能与规模（重点）

#### watch 上限（issue #65 / #202）

原版每目录一个 watch，而默认 `max_user_watches` 远远不够，这是这条路线的硬墙。社区一直引用「约 1KB/个、不可换出」，本机实测（§6.2）是**约 100 字节不可换出 + 约 900 字节可回收的 inode/dentry** —— 前者是真正的墙，后者在内存压力下会被回收。

- `watcher.Backend` 是接口，`inotify` 只是其一：**换 `fanotify`（整挂载点单 watch）或分片多实例只需新增一个文件**，不动引擎。
- `walkAdd` 用 worker pool 并行注册（默认 `min(NumCPU, 8)`），而不是原版的单线程递归 —— #202 里「125 万目录单核跑几天」直接变成线性加速。
- `ENOSPC` 被包装成 `...: raise /proc/sys/fs/inotify/max_user_watches`，不让用户面对裸 errno。

#### 事件聚合（issue #202 / #695）

原版：delay 队列线性扫描去重（O(n²)，维护者自认）、硬上限 1000、`Combiner` 逐对比较。10 亿文件的一次批量 `mv` 会产生百万级事件 → 每 1000 条 fork 一次 rsync。

新实现（`internal/aggregate`）：

- `map[path]index` 定位，每事件 `O(1)`；
- 合并规则：`delete→create` 折叠为 `write`；`create→delete` **整条消失**（写临时文件再 rename 的典型突发从 3 条变 1 条）；目录 `delete` 用 `dropSubtree` 吸收全部子路径；`move` 抹掉源路径的待处理项；
- **目录索引**：`dropSubtree` 与 `followRename` 只走被删/被改名的子树，不扫整个窗口。第一版是全窗口扫描，于是「删 N 个目录」= N × 窗口大小 —— 窗口 20 万文件时折叠一次要 **58.4 秒**，50 万（默认 `max_pending`）直接超时。而这期间 `Add` 一直持锁，watcher 灌不进新事件，内核队列必然溢出（issue #695 的真实成因）。改成显式目录树（`nodes map[string]*dirNode`，节点在有条目时创建、子树空了自底向上回收）后，同样场景降到 **23.8ms / 55.1ms**，严格线性。实测：

  | 窗口内文件数 | 待删目录数 | 修复前 | 修复后 |
  |---|---|---|---|
  | 10,000 | 2,000 | 64.4ms | 2.47ms |
  | 100,000 | 20,000 | 13.3s | 10.6ms |
  | 200,000 | 40,000 | 58.4s | 23.8ms |
  | 500,000 | 100,000 | 超时 >120s | 55.1ms |

  目录重命名同理（50 万文件 = 37.3ms）。

  上表的「修复后」是本机（Windows，Ryzen 7）的数字。在真机上（Anolis 7.9 / 8 核 / kernel 4.19.91，`/root/gs18`，`benchtime 1x`）重测同一组基准，绝对值高三到五倍但**线性依旧** —— 这才是 10T 场景要用的数：

  | 窗口内文件数 | 待删目录数 | 本机修复后 | 真机修复后 | 真机每目录 |
  |---|---|---|---|---|
  | 10,000 | 2,000 | 2.47ms | 4.09ms | 2.05µs |
  | 100,000 | 20,000 | 10.6ms | 60.4ms | 3.02µs |
  | 200,000 | 40,000 | 23.8ms | 113.8ms | 2.84µs |
  | 500,000 | 100,000 | 55.1ms | 292.6ms | 2.93µs |

  每目录 2–3µs 且不随规模上升，说明折叠一次批删的代价只与「真的删了多少个目录」成正比。真机上的目录重命名同形状：10 万文件 54.1ms、50 万文件 271.6ms（543ns/文件）。换算到 10T：一次 `rm -rf` 删掉 10 万个目录，聚合层只花 **0.29 秒**，而修前是 >120 秒且期间持锁 —— 这就是 issue #695 里「卡死」与「不卡死」的分界。代价是每条 `Change` 多两个非导出字段（`parent`/`slot`）和一个 `map` 条目，插入仍是 O(1)（`BenchmarkAddLarge`：窗口 1000 时 600–800ns/op，窗口 10 万时 1100–1200ns/op）。
- 上限 `MaxPending` 默认 50 万，超出只置 `urgent` 标志 —— **不丢事件**，引擎立即把当前窗口送出去（而不是等 delay 到点），也不把它当成数据丢失去触发全量重传。原版这一层是硬上限 1000 + 静默丢弃（issue #202）；
- `Flush()` 按路径排序返回，使同一批变更生成的 rsync 参数可复现。

#### 目录删除的簿记代价（issue #695，与上面同类的二次方）

聚合层修好了「一次批删折叠成一条」，但 watcher 里还留着一份同样形状的二次方：`rm -rf` 每删一个目录发一条 `Delete` 事件，而 `Tree` 收到目录删除后要问「这个路径下面还有哪些已记录的目录」，第一版是用 `forgetBelow` **扫整张 watch 表**。单次扫描是毫秒级，注释里也据此写了「一百万条目仍然只要几毫秒，远小于它回收的内存」—— 但乘上目录数就是 O（目录数²）：

| 已 watch 目录数 | 修前每次删除 | 修后每次删除 |
|---|---|---|
| 10,000 | 157 µs | 145 ns |
| 100,000 | 1.54 ms | 148 ns |
| 1,000,000 | **44.1 ms** | **229 ns** |

（`BenchmarkLeafDelete`，本机 benchtime 2000x；`BenchmarkMassDeleteTree` 1000 个目录整棵删除：7.92 ms → 294 µs。）

按修前的数字外推，100 万目录的一次 `rm -rf` 要在 watcher 里烧掉**约 12 小时 CPU**，而这段时间它腾不出手去排空内核队列 —— 溢出、全量重扫、重启循环，这就是 issue #695「批量操作时卡死」的真实成因，和聚合层那个 58 秒是同一个病的两个病灶。

修法是给 watch 表加**子节点索引**（`internal/watcher/bookkeeping.go`）：`dirs` 从 `map[string]int` 改成 `map[string]int32`（值是它在父节点 `kids` 切片里的槽位），`kids map[string][]string` 记录每个已记录目录的子目录。于是：

- 删一个叶子目录是 O(1)（槽位交换摘除）；
- 删一棵子树只走这棵子树，代价与真正删掉的东西成正比；
- 目录重命名（`rekeyDirs`）先把整棵子树从索引里摘掉、再按「父先子后」重新登记 —— 原地改写会让子节点仍挂在旧名字下，而 inotify 的 watch 属于 inode，改名之后内核继续用同一个 wd 报事件，簿记错位就会让后续事件带着不存在的路径，静默把目标端删掉。

槽位可能失效（手工登记、并发路径），此时回退成只扫**一个父节点**的 `kids`，复杂度受扇出限制而不是表大小。

这类索引最容易「快但说谎」，所以配了一个随机对照测试 `TestIndexMatchesReference`：随机建 200 节点树、30 轮 × 40 步随机混用 `dropPath` / `forgetBelow` / `takeBelow`，每一步都把索引与「纯 set + 全扫」的参照实现逐条比对，并断言 `OrphanCount() == 0`（没有条目丢掉父链接 —— 丢了就再也删不掉，是长期运行的守护进程里 `watched_dirs` 单调上涨的来源）。这个测试在开发中确实抓到两个 bug：摘除时无条件回写槽位会**把已删条目复活**成孤儿，以及 `dropPath` 用在非叶子目录上会留下无法回收的后代。

#### 批量删除时 rsync 会「失败」（issue #695 的第二个病灶）

上面两处修的都是我们自己的簿记，但批删还有一个来自 rsync 的陷阱，而且它看起来完全不像 bug —— 它看起来像「网络有问题」。

rsync 把「部分传输」报成非零退出码 —— 手册里 23 是「部分文件无法读取」，24 是「读取前源文件已消失」。`rm -rf` 一个百万文件的子树时，**每一个批次都会撞上它**：批次里的路径是几毫秒前列出来的，rsync 真正去读的时候其中一部分已经不在了。

真机实测（rsync 3.1.2）把这件事钉得很具体，顺带纠正了一个想当然：

| 构造方式 | rsync 实际退出码 |
|---|---|
| `--files-from` 列表里有 2 个根本不存在的路径 | **23**（不是 24） |
| 列表里存在、rsync 传输过程中被并发删掉 | **23** |

两种形状都返回 23。24 是更窄的一种竞态（rsync 已经 stat 过、读之前消失），在 4KB~8KB 的小文件上极难人工触发 —— 但这不影响判断：两个码都必须按「部分传输」处理。于是：

1. 每一批都返回非零退出码，被当成硬失败；
2. 引擎把失败批次重新塞回聚合窗口，并给这个 sync 挂上指数退避（15s→10min）；
3. 因为窗口非空，下一次刷新又重复同样的事；
4. 而 `apply` 在失败路径上会 `MarkOverflow()`，也就是**排一次全量重扫** —— 为了一次其实已经把「当时还在的文件」全部送达的传输。

把这四步乘上一棵 10T 的树，得到的就是 issue #695 描述的「批量操作时卡死」，以及用户实际看到的「删了 20 万个文件，lsyncd 卡了两小时然后开始重扫全树」。

修法是把退出码分类，而不是把「非零」当唯一判据：

| 退出码 | 含义 | 处理 |
|---|---|---|
| 0 | 成功 | 成功 |
| 23 | 部分传输，某些路径读不了 | **按成功处理**，打一条 warn（它也可能是真的读不了，运维需要看到一次） |
| 24 | 部分传输，源文件已消失 | **按成功处理**，不打印（批删期间这是常态，打日志就是刷屏） |
| 其他 | 用法错误、IO 错误、被信号杀掉 | 失败，进入退避与重试 |

实现上是 `internal/sync/proc.go` 里把 `exec.ExitError` 包成带类型的 `ExitError`（带 `ExitCode(err)` 取值函数），后端的 `benignExit` 只对 23/24 返回 true。**已经送达的路径照样计入 `files` 计数** —— 部分传输仍然搬走了活下来的那些文件，不计数会让大规模删除时的进度看起来是停的。

两条测试钉住它：`TestBenignExitCodes`（表驱动，含 0/1/23/24/12/255/-1）与 `TestApplyTreatsExit24AsDelivered`（真起一个 `exit 24` 的假 rsync，断言 `Apply` 返回 nil 且 `Files == 2`）。后者在 Windows 上会 skip（需要 POSIX shell），真机上两条都跑过。

端到端复测（/root/RS2,40 个顶层目录 × 50 文件 = 2000 文件，并发 `rm -rf` 掉 30 个顶层）：目标端从 2000 收敛到 **500 文件 / 10 目录**，计数 `deletes=30 errors=0 retries=0 full_syncs=0 overflows=0 batches=1`，日志里 `partial transfer` 与失败行都是 0。

**这个场景里退出码根本没被触发**，因为本地目标的删除走的是 `deleteList` 里的 `os.RemoveAll`，压根不经过 rsync。分类逻辑真正保护的是**上传路径**：当一批要传的文件在批次排好队之后被删掉（`rm -rf` 与「往同一个子树写新文件」并发时很常见），rsync 会带着部分成功退出。换句话说这条修复是**防御性的**，不是这次实测发现的故障 —— 如实写在这里，免得后文被读成「已经复现过一个 23/24 引发的卡死」。

顺带一提，`ExitError` 也是 issue #30（「rsync 的 IO 错误被误判为成功」）的反面：那次是**太宽松**，这次是**太严格**。两者都要靠「区分具体退出码」来解决，而不是靠一个布尔值。

#### 每次同步一个 rsync（fork 风暴）

新实现：

- 默认 `direct` 后端：进程内拷贝，零 `fork`；写 `.gsyncd-tmp-*` → `rename`，目标端永远看不到半个文件。`fsync` 只对 8 MiB 以上的文件做（否则百万小文件的刷盘开销超过拷贝本身），要全量刷盘就设 `fsync = true`；
- `rsync` / `rsyncssh` 后端把变更列表通过 `--files-from=- --from0` **走 stdin**，而不是拼进 argv —— 原版拼 argv 会撞 `ARG_MAX` 并静默截断大批次；超过 `batch_limit`（默认 1 万）才分片；
- 全局 `max_processes` 信号量：所有 sync 共享，跨 sync 也不会 fork 风暴；每个 `[sync]` 还能设自己的 `max_processes`，否则一个繁忙的 sync 会占满全部名额，小而被动的 sync 只能排在它后面。
- 并发度是**两个独立的旋钮**：`max_processes` 限的是「同时在飞的传输批次」数，对所有后端生效 —— 对 rsync 后端它就等于并发 rsync 子进程数（每个都要 fork 并自己走一遍目录树，所以几个就够）；direct 后端每个 sync 本来就一次只跑一批（`flushAsync` 的 `flushing` 标志），所以 per-sync 的 `max_processes` 在 direct 下几乎不会成为瓶颈，direct 的并行度由 `workers` 决定（拷贝 goroutine，不 fork、不各自走树，便宜得多）。实测两者合理的取值区间完全不同，合成一个键会让人调错方向 —— 见 §6.1。

#### 溢出即重启（内核队列溢出）

原版 `IN_Q_OVERFLOW` → `runner.overflow()` + `hup = 1`，整套进程重启、事件全丢、全量重扫（100T 下是小时级）。

新实现：`Tree` 把 `event.Overflow` 原样透传 → `engine` 调 `agg.MarkOverflow()` → 该 sync 下一批 `Batch{Full: true}` **只做一次全量重传，进程不重启、watch 不重建**。

#### watch 表打满即整体不可用（issue #65）

原版 `inotify_add_watch` 返回 `ENOSPC` 就当致命错误，进程退出 —— 目录数超过 `/proc/sys/fs/inotify/max_user_watches` 的树根本起不来（#65 里 17G 数据只认到 7933 个目录就止步）。

新实现：

- `watcher.ErrWatchLimit` 是**可降级**条件：`Tree.addOne` 捕获后计数 `unwatched` 并继续遍历，已经注册的目录照常工作；
- `main` 在装完 watch 后检查：`unwatched > 0` 就自动启用 `full_resync`（默认每小时一次），用周期性全量重同步盖住看不见的子树；显式配置 `full_resync` 则一律按它跑（0 与不写等价，都表示「只在有目录装不上 watch 时才每小时一次」，没有「彻底关闭」的取值）；
- `-status`/状态文件把 `watched_dirs` 与 `overflows` 一起暴露，便于判断是否该加 `full_resync` 或调 `max_user_watches`。

代价说清楚：降级意味着未 watch 的子树在两次全量重同步之间**不一致**，只能靠全量兜底。这是对的取舍 —— 原版的替代方案是直接不工作。

#### MOVE 降级（issue #225）

原版：inotify 缓冲区读完仍没配对的 `MOVED_FROM` 就当「移出监视树」→ 降级 Delete+Create。`mv` 一个 1TB 目录 = 目标端全量重传。

新实现（`internal/watcher/inotify_linux.go`）：

- `pending map[uint32]pendingMove`，按 **cookie** 精确配对；`MOVED_TO` 命中则发一条 `Event{Path: 新, From: 旧, Op: Move}`；
- 超过 `MoveGrace = 750ms` 仍未配对的才降级为 `Delete`（用 `SetReadDeadline` 轮询顺带 flush，不额外开 goroutine）；
- 聚合层再把这一个 move 折叠成「目标端一次 `rename`」，而不是删除 + 重传。

链路上另外两处必须跟着改，否则「配对成功」反而变成数据丢失 —— 这两条是移植时自己踩出来的：

1. **watch 表要跟着改名**(`inotifyBackend.Rekey` + `Tree.rekey`)。inotify 的 watch 属于 inode，`mv old new` 之后内核继续用同一个 wd 报事件，但我们映射回名字的表还停在 `old`；此后该子树下每个事件都带着一个不存在的路径，引擎会老老实实把它镜像成「删除」→ **重命名一个目录会静默删掉目标端的副本**。改名时同步重写 `wdToDir` / `dirToWd` 与 `Tree.dirs`。

   这一条的第一版实现是「遍历整张 `dirToWd` 找前缀」，和 §3.1 里批删那个二次方是同一类错误：一次 `mv` 要扫全部已 watch 目录，而且**全程持有读循环的同一把锁** —— 扫的这段时间 inotify 事件在内核队列里干等。真机实测（Xeon E5-2696 v3，`BenchmarkRekeyLargeTree`）：

   | 已 watch 目录 | 修前 | 分配 |
   |---|---|---|
   | 10,000 | 5.9 ms | 0.9 MB |
   | 100,000 | 89 ms | 12.8 MB |
   | 1,000,000 | **1.145 s** | **122 MB** |

   百万目录上重命名一次目录要 1.1 秒，而目录重命名在真实负载里并不罕见（日志轮转、rsync 的临时目录交换、运维搬数据集）。修法是把「哪些路径跟着改名了」这件事交还给本来就知道答案的一方：`rekeyDirsLocked` 从 `Tree` 的子节点索引里直接取出子树并返回，调用方在**释放锁之后**把这份列表交给后端，后端只改写列表里的条目。后端的 `Rekey` 签名因此变成 `Rekey(from, to string, moved []string)` —— 它自己去找，等于每次重命名扫一遍全表，而那些路径调用方手里本来就有。

   契约由两条用例钉住：`TestRenameTellsBackendWhichPathsMoved`（子树完整、不含兄弟、父先子后、只调一次）与 `TestRenameWithoutRecordedSubtreeSkipsBackend`（没登记过的子树一次都不调，不为空列表分配）。真机内核侧的 `TestRekeyFollowsRenameOnRealKernel` 则直接验证效果：改名之后往新名字下写文件，事件必须以新名字到达 —— 以旧名字到达就是上面那条「静默删掉目标端副本」。
2. **待处理项要跟着改名**（`Aggregator.followRename`）。旧路径下挂着的 `create`/`write` 已被 rename 本体覆盖，直接丢弃（否则十万条已不存在的路径被当成删除）；但挂着的是**纯删除**时要重定向到新名字 —— rename 带走的是内容，不带走一次已经发生的删除，丢掉它会让那个文件的副本永远留在目标端。

#### 新建子目录竞态（issue #284）

原版先启动 rsync 再装 watch，且新目录的事件驱动加 watch 存在窗口。

新实现：

- `main` **先 `tree.AddRoot` 装好全部 watch，再 `eng.Startup`** 做初始同步；
- `poll` 后端额外提供 `Prime()`，在初始拷贝前拍基线，保证拷贝期间的写不会被并入基线；
- 新目录在 `handle()` 里交给固定的 adopt 池（2 个 goroutine），由 `adoptTree` 扫描整棵子树；**扫描出的既有文件与子目录会合成 `Create` 事件回填**。少了这一步就只是把竞态窗口从一个位置挪到另一个位置：`mkdir` 与第一个写入之间只隔几微秒，而 `add_watch` 要等目录事件穿过 `pump` 才执行，这期间创建的文件不会有任何 inotify 事件 —— 内核只报告 watch 存在之后的变更。

#### 排除规则的一致性

原版的 exclude 只在两处生效：事件侧过滤，以及转发给 rsync 的参数。于是「被排除的目录」在启动扫描阶段照样被遍历一遍（目录多大就扫多大），而且 `/cache/` 这种目录模式**只匹配目录条目本身** —— 之后在 cache 里新建的文件照样进队列。

新实现把同一份列表接到三层：

1. **watch 层**：`watcher.Options.Ignore` 让被排除的目录根本不进 watch 表，省的是内核内存与 `max_user_watches` 配额（#202 / #65）；
2. **事件层**：`filter.Filter.Match` 在路由后过滤；
3. **传输层**：`direct` 在遍历时 `filepath.SkipDir`，`rsync` 后端把规则逆序渲染成 `--exclude=` / `--include=`（rsync 是首个命中即停，本地是后者优先，逆序后两边语义一致）。

另外三个必须处理的细节：

- **目录模式覆盖其内容**。`cache/` 现在匹配 `cache` 本身**以及它下面的一切**（对文件只匹配真祖先，所以叫 `tmp` 的普通文件不会被 `tmp/` 误伤）。rsync 从不进入被排除目录，FS 侧也应该一致。
- **分隔符归一化**。模式永远用 `/`，而 Windows 上 watcher 给出的是 `\`。`Match` 入口统一 `ReplaceAll(rel, "\\", "/")`，否则整份 exclude 在 Windows 上会静默失效 —— e2e 里就是靠这条断言抓出来的。
- **rename 回退路径要重定基**。目录从树外 `mv` 进来、或跨设备改名时，目标端没有旧名字，`applyRename` 会回退成 `copyTree` 整棵拷贝。`copyTree` 拿到的相对路径是相对**被拷子树**的，而 `Ignore` 要的是相对**同步根**的 —— 不重定基（`filepath.Join(relTo, rel)`），规则会静默匹配不到任何东西，`mv` 一个父目录就把整片 `cache/` 偷渡到目标端。测试 `TestRenameFallbackCopyHonoursIgnore` 就是钉这条的。

#### 拷贝请求的大小（`internal/sync`）

一条只在真机上才暴露的税：`copyFile` 原先用 `io.CopyN`，而 `io.CopyN` 会用 `LimitReader` 把源包一层 —— 这一层既杀掉了 `*os.File` 的 `copy_file_range`/`sendfile` 快路径，又让剩下的通用路径退回 **32 KiB** 缓冲。代码上看不出任何异常，只有 `/proc/<pid>/io` 会说话：一次 10T 级的初始同步里 `rchar 34.0 GB` 对应 `syscr 1,204,339` 次读，**平均 28.3 KB 一次系统调用**。

同一块盘上 `dd` 100 MiB `conv=fsync` 的实测（逐档删除，盘空闲）：

| bs | 用时 | 吞吐 |
|---|---|---|
| 32k | 6.52 s | 16.1 MB/s |
| 128k | 7.89 s | 13.3 MB/s |
| 1M | 4.29 s | 24.4 MB/s |
| 4M | 3.05 s | **34.3 MB/s** |

请求越大越快，32k 到 4M 差 2.1 倍 —— 限速的是每次请求的固定开销，不是带宽。所以 `copyFile` 改用 `copyBounded`：`sync.Pool` 里取 1 MiB 缓冲（百万文件的树不会给 GC 递上百万个 1 MiB 分配），按 `stat` 到的长度有界拷贝，读完把切片长度还原到容量再放回池 —— 不还原的话下一次用这条池化缓冲的拷贝会退化成按上次的小文件长度切片，把这次改动的收益悄悄还回去。1 MiB 是曲线变平的位置；`TestCopyBoundedReturnsFullBufferToPool` 钉的就是「池里的缓冲必须原样回来」。

本机（Windows，写本地磁盘，64 MiB）对照：`copyBounded` **3102 MB/s** vs `io.CopyN` **2370 MB/s**。真机 A/B 见 §6.2。
真机上的 A/B 就没这么好看了，而这个「不好看」本身就是结论。同一块虚拟盘、同一棵 210 文件 / 42 MB 的子树、`copybench` 4 worker：先被页缓存骗了两次（42 MB 全在缓存里，两版都跑出 700–1200 MB/s，而设备只有 30 MB/s），改成每轮 210 MiB 且每轮前 `drop_caches` 之后，三轮仍然分不出胜负（±30% 噪声）。最后用 ABBA 交替顺序跑 5 轮 × 各 10 次：

| 版本 | 中位数 | 均值 | 最好 | 最差 | 中位吞吐 |
|---|---|---|---|---|---|
| 0.1.14(`io.CopyN` 32 KiB) | 6,096 ms | 6,380 ms | 5,727 ms | 8,016 ms | 34.4 MB/s |
| 0.1.15(`copyBounded` 1 MiB) | 5,754 ms | 6,071 ms | 4,991 ms | 8,906 ms | 36.5 MB/s |

**中位数快 5.6%，落在噪声里。** 原因是这台设备的顺序写上限本来就只有 30–36 MB/s:1 MiB 请求把每次系统调用的固定开销摊薄了，但设备已经饱和，省下的开销换不成吞吐。所以这一项的正确说法是「**它不伤性能，并且在更快的存储上会把 dd 曲线里那 2.1 倍的差距兑现**」，而不是「在这台机器上快 2 倍」。要在这台机器上再快，只有换盘。

#### 排除规则的热路径：缓存是负收益（`internal/filter`）

`Match` 对遍历的每个条目、每个事件都跑一次，所以它是真正的热路径。原实现用一个 `map[string]bool` 缓存「完整路径 → 是否排除」，并为此每次调用锁两次全局互斥量。实测（本机 AMD Ryzen 7 H 255，benchtime 200000x）：

| 场景 | 走缓存 | 不走缓存 |
|---|---|---|
| 全唯一路径，串行 | 1214 ns/op | **872.2 ns/op** |
| 全唯一路径，并行 | 190.0 ns/op | **187.1 ns/op** |

唯一路径的命中率是 0：值得优化的树恰恰是每条路径都只问一次的树，加锁与 map 写入全是纯开销。另外测出 20 万条上限下缓存常驻 **13 MiB**（68 B/条），而这些字符串再也不会被问第二遍。所以缓存被**删掉**，而不是调参。

真正的改进在别处：

1. **只 Split 一次**。原实现每个模式各 `strings.Split(rel, "/")` 一次，8 条规则 = 一次判定 8 个切片分配；现在 `Match` 里 Split 一次后传给所有模式，且**惰性** —— 只有遇到需要逐段的模式才真的 Split。
2. **单段模式走零分配快路径**。`*.tmp`、`node_modules` 这类是用户实际会写的规则，`Pattern.single` 为真时 `matchSingle` 直接沿分隔符扫，不分配切片。目录模式（`tmp/`）只匹配真祖先，所以名为 `tmp` 的普通文件不会被误伤。
3. `ReplaceAll(rel, "\\", "/")` 前置 `IndexByte` 判断，Linux 上直接跳过这次扫描。

结果：8 条单段规则的典型路径 **508.3 ns/op、0 B/op、0 allocs/op**（并行 81.41 ns/op）；混合规则 `BenchmarkMatchUniquePaths` 444.9 ns/op / 48 B/op。语义由 `TestSingleFastPathAgreesWithGeneral`（10 个模式 × 24 条路径 × dir 两值逐条与通用路径比对）与 `TestFastPathKeepsDescendantRule` 钉住 —— 快路径若改变了排除语义，表现只是「该忽略的文件被同步了」，不会报错。

#### 初始扫描失控（issue #75）

- 扫描与传输共享同一个信号量，多个 sync 不会同时把磁盘打满；
- `-once` 模式可以直接串行化运维窗口；
- 传输与扫描分离后，断点续扫就是在这条缝里补一个持久化游标 —— 已经实现（见 §3.2「断点续扫」，§5 第 3 条已划掉）。

#### 网络故障 100% CPU(issue #250)

新实现 `sync.Backoff`：`base << attempt`，再加 ±20% 抖动避免多实例同步重试。它有两个调用点，上限不同：**失败的整轮**由引擎的 `retryAt` 门控制，`Backoff(fails-1, 15s, 10min)`，从 15 秒涨到 10 分钟封顶；**单批传输失败**走 `applyRetry`，`Backoff(attempt-1, 1s, 5min)`，1 秒起、5 分钟封顶。失败批次通过 `readd()` 重新放回聚合窗口 —— **只重试这些路径，不是全树**。

#### 日志（issue #93,438GB 日志）

原版每条日志 `fopen` + `ctime` + `fprintf` + `fclose` + 一次 `syslog`。

新实现 `logx`：

- 单个带缓冲的 writer，进程生命周期内不再重复 `open`；
- 按 `log_max_size` 轮转，保留 `log_max_files` 个（`app.log.1` …）；
- `Throttlef(key, interval, level, format, ...)`：**同一 key 在 interval 内只打一次**，末尾附 `(suppressed N similar messages)`。用于「waiting for N child processes」这类原本会刷满磁盘的周期性消息。

#### 状态持久化

原版状态全在内存（延迟队列、`wd → path`、exclude 编译结果），崩溃/重启即从头全量。

新实现 `internal/status`：周期性把统计写成小 JSON，`tmp` + `os.Rename` 原子替换（POSIX 与 NTFS 都原子）；`-status PATH` 直接读给人看。

**状态文件必须在装 watch 之前就存在，而且要说清自己在干什么。** 这两条都是真机上被数据逼出来的，不是设计时想到的：

十 TB 的树上，启动到「稳态」之间隔着两段等待 —— 先是装 watch（纯装表、目录项已在 page cache 里时 7.4 万/秒，5000 万目录 11 分钟；冷缓存走真实磁盘时 1.8 万/秒，505 万目录实测 4m40s；若是边 `readdir` 边装，100 万目录 92.9s、420GB 树上 220,101 个目录 421s），然后首次全量（4.3–46 小时，这一段才是小时级）。第一版把 status writer 放在 `eng.Startup` 之后启动，于是**运维最需要看进度的那几个小时里文件根本不存在**；第二次修成「Startup 之前启动」，但装 watch 还在更早的 `tree.AddRoot`，于是那段时间文件存在、内容却是全零 —— 一个满速装 watch 的进程和一个卡死的进程在 JSON 里长得一模一样。第三次才把 writer 提到 `AddRoot` 之前，并加 `phase` 字段区分：

| `phase` | 含义 | 期间计数器 |
|---|---|---|
| `watching` | 正在装内核 watch，`watched_dirs` 在涨 | 全部为 0 |
| `initial` | 首次全量在跑 | 全部为 0（`files` 除外，它由后端记） |
| `running` | 事件循环接管 | 正常 |

真机验证（/root/PH3,13.2 万目录，先 `drop_caches`）：`t=1.7s phase=watching watched_dirs=417` → 一路涨到 `t=19.3s watched_dirs=3897`，之后才转 `initial`。20 万目录的热缓存树上这个阶段只有 1 秒，所以**只在冷缓存的大树上才看得到** —— 这也是它值得单独一个字段的原因：越是冷、越大、越慢的树，越需要这个标签。

真正的状态持久化在 `internal/scanstate` —— 见下一小节。

#### 断点续扫：重启不再重扫（`internal/scanstate`）

上面那条「状态全在内存」的代价可以量化：实测重扫已经同步好的 100 万文件是 **2,336 文件/秒**（只比 `size`+`mtime`），外推到 10T 的 5000 万文件就是 **5.9 小时纯扫描，一个字节都还没传**。这在运维上意味着「重启一次要预留 6 小时」。

慢的原因不是要比对内容，而是**要重新证明一遍**：源端每个目录 `readdir`、目标端每个目录 `readdir`、每个文件 `stat` 一次目标端。断点续扫要做的就是把「这个目录已经和目标端一致」这件事持久化，重启时只验证、不重做。

实现分三块：

**记录格式**（`scanstate.Store`）。按 walk 顺序追加的定长小记录，每条 `{Path, Mtime, Count, Hash}`：`Path` 相对同步根、`Mtime` 是源目录自身 mtime 的 UnixNano、`Count` 是直接子项数、`Hash` 是直接子项的摘要（子目录只折名字，文件折名字+size+mtime）。前面是 `magic + 配置指纹` 两行头，每条记录带 uvarint 长度与 CRC32。

**合并靠前向游标，不靠 map。** `Lookup(rel)` 只在上一轮的记录流上前进，不回头 —— 因为 walk 访问目录的顺序与上一轮写记录的顺序一致。5000 万目录的 `map[string]Record` 是数 GB 内存，游标是 O(1)。顺序比较必须按**路径分量**而不是字节序：`a-b` 的字节序排在 `a/b` 前面（'-'=0x2d < '/'=0x2f），但 `filepath.Walk` 先下潜 `a` 再访问 `a/b`（`walkLess`/`splitComponent`）。

**跳过条件（三条同时成立）**：源目录的 mtime、子项数、子项摘要都与记录一致；目标目录存在且其 mtime 等于记录的源目录 mtime；检查点指纹匹配（源、目标、exclude、模式都没变）。命中则该目录**完全不碰目标端**：不 mkdir、不 readdir、不逐文件 stat、不 prune，连扣在内存里的待拷文件也直接丢弃。

第二条是关键设计。每次拷完一个目录，`pending.finish()` 用 `os.Chtimes(dst, mtime, mtime)` 把**目标目录的 mtime 设成源目录的 mtime**（与 `rsync -a` 一致）。于是任何事后动过镜像的操作 —— 手删文件、从旧备份恢复、目标盘崩过 —— 都会改变目标目录 mtime，让记录当场失效，而不需要一个额外的「目标 mtime」字段。早先的设计存过目标目录自己的 mtime，但那样必须等该目录最后一个文件落地才能读，一个目录里有一个超大文件就会卡住整条 walk；改成「目标目录 mtime := 源目录 mtime」之后，`pending` 变成 fire-and-forget，不再阻塞。

摘要里子目录只折名字、不折子目录的 mtime，也是同一个道理：否则一个叶子的一次写会让整条祖先链失效，持续写入的树上检查点永远命中不了。子目录的变化由它自己的记录覆盖。

**正确性的唯一来源是「记录只是加速器，永远不是证据」。** 文件缺失、被截断、CRC 不符、指纹不符、路径不可写、`<path>.tmp` 是个目录 —— 每一条失败路径都退化成「没有检查点」，多走一遍全量，绝不会少走。记录在目录**拷完之后**才写，所以被杀掉的进程留下的尾部最多少记一批，不会记进没做完的活（`Finish(false)` 不晋升，但下一轮仍会收养那段 `.tmp`）。`Add` 每 4096 条 `Commit` 一次，但 `Commit` **只 `flush` 不 `fsync`**：每条记录描述的都是已经拷完的目录，丢一条只意味着下次多验证一次（安全方向），而每几千条付一次 fsync 比它省下的验证更贵 —— 21 万目录的树上 51 次 `fsync` 把 23 秒的热启动拖成 45 秒。真正需要持久化的是 `Finish` 里的那次 `rename`，那一步仍然 `fsync`。

一个实现细节：记录是**后序**完成的（一个目录的子项集合要等整棵子树走完才定），而 store 只能按 walk 顺序追加，所以中间有一个 `queued []scanstate.Record` 缓冲，超过 `maxQueuedRecords`（32768）就从头丢弃并计入 `dropped` —— 少记一些目录，下次多验证一些，仍是安全方向的失败。同理，一个目录在摘要未定期间最多扣住 `maxHeldFiles`（16384）个文件，超过就当作已改变、正常流给 worker。

日志一行：`scan state: 1000001 directories verified, 1000001 reused, 0 recorded, 0 dropped`。

#### 冷启动遍历：串行 walk 才是瓶颈（`sync/prewalk.go`）

先量清楚钱花在哪。21 万目录、20 万文件的树，冷缓存下把冷启动拆成三个原语（`probe`）：

| 原语 | 耗时 | 说明 |
|---|---|---|
| 源端遍历（每项 `walk` + `stat`） | **139.7 秒** | 占整个冷启动的 99%；这个探针每项多做一次 `stat`，所以比下面 `filepath.Walk` 的 126.5 秒多 |
| 目标端每目录 `readdir`（无检查点时做的事） | 3.1 秒 | |
| 目标端每目录 `stat`（有检查点时做的事） | 0.9 秒 | |

也就是说：冷启动的成本几乎全在**源端串行遍历**上，目标端的两种验证方式都只是噪声。这也解释了断点续扫在冷缓存下为什么倒亏 —— 它优化的是那 4 秒，却要多写一段约 10MB 的检查点（48.6B/目录 × 21 万目录）。

串行遍历慢的原因不是 CPU，是**同一时刻只有一个元数据请求在飞**：`filepath.Walk` 读完一个目录、处理完、再读下一个，磁盘队列在两次请求之间是空的。

修法是**保序并行预取**，不是换成无序并行遍历。访问顺序是硬约束 —— 检查点是按 walk 顺序追加的前向游标，顺序一变记录就全部错位。所以 `prewalk.go` 的做法是：

- N 个 worker 只做「把某个目录读成 `[]dirEntry`」；
- dispatch goroutine 收到结果后，对该目录的每个子目录发一层扇出请求，用 `window`（默认 256 个列表）与 `prefetchEntryBudget`（262144 个条目）双重限流；
- 驱动递归仍按 `os.ReadDir` 的排序结果逐个下潜，**顺序与 `filepath.Walk` 逐条一致**，预读只改变「列表何时被读到」，不改变「下一个访问谁」。

冷缓存实测（每轮前 `drop_caches`，同一棵树）：

| 变体 | 21 万目录 / 20 万文件 | 21 万目录 / 100 万文件 |
|---|---|---|
| 串行 `filepath.Walk` | 126.5 秒 | 131.1 秒 |
| 并行 w8 | 24.1 秒 | 56.2 秒 |
| **并行 w16** | **15.9 秒** | **50.6 秒** |
| 并行 w32 | 23.8 秒 | 59.2 秒 |

**快 5–8 倍，w16 是拐点，w32 退化**（元数据争抢）。文件多时收益变小：瓶颈从目录 inode 转向文件 inode。

三个必须踩过的坑，都写在代码注释里：

1. **窗口满时不能静默丢请求**。第一版 `request()` 返回 void，窗口满就直接丢弃，而 `fetch()` 已经注册了 waiter 等 5 秒超时 —— 宽树上触发上千次，整轮跑了一分半还没结束（串行 65 秒已结束）。改成返回 bool，`fetch` 在「池子不会读它」时自己内联读，决不等超时。
2. **预读必须能关**。`prefetch = 0`（默认）是自动，`prefetch = -1` 退回纯 `filepath.Walk`。预读 worker 与拷贝 worker 争的是同一块盘，冷启动时读得比拷得快没有意义；在已经跑满的机器上关掉它更划算。
3. **预读的代价按条目算，不按目录数算**。第一版只用 `window`（4096 个目录列表）限流，而列表的条数说明不了它有多大：500 万文件、每目录 500 个文件时，4096 个已读未用的列表 × 500 条 ≈ 数百万个 `DirEntry` + `FileInfo`，实测把 RSS 顶到 **984MB–1.08GB**（`VmHWM` 1,076,996kB），而进程真正在干的活只需要几十 MB。修法是加一个**条目预算** `prefetchEntryBudget = 262144`：dispatch 每扇出一层前先看已缓冲的条目数，超预算就停止请求，没被请求的目录由驱动内联读（一次系统调用，本来也要做）。默认窗口同时从 4096 降到 **256** —— 投机只需喂饱 worker，一个 worker 手上有 16 个目录列表在飞就够，更大的窗口只是把几十秒后才用到的目录提前读进内存。`TestPrefetchStopsAtBudget` 钉住这条边界：预算耗尽时扇出必须一次请求都不发。

端到端效果（冷缓存，`-once`，同一棵 21 万目录 / 20 万文件的树）：

| | 串行 | 预读 16 |
|---|---|---|
| 无检查点 | 636 秒 | **556 秒**(−12.7%) |
| 有检查点 | 751 秒 | **558 秒**(−25.7%) |

注意预读把「有检查点」拉回到与「无检查点」持平 —— 冷缓存下检查点仍然不赚，但它也不再亏 115 秒了。

#### 单线程

watcher goroutine → 有界 channel → engine ticker（`Tick` 默认 200ms）→ 每 sync 一个 delay 定时器 → 传输由信号量限流。折叠、过滤、传输均可并行，且不需要用户调线程数。

### 3.3 只有真机才暴露的缺陷

下面五条全部是**在 Linux 真机（XFS、8 核）上压测时才发现**的：Windows 开发机上当时全部用例全绿、e2e 全通过，换到目标内核后行为完全不同。列在这里是因为它们比 §3.1 那些「读代码就能看出来」的缺陷更值得记住 —— 跨平台的文件系统语义差异无法靠单测覆盖，只能靠真实内核 + 真实文件系统 + 有规模的树。

#### 事件才是权威，`stat` 只能当提示（`sync/direct.go` 的 `applyOne`）

原逻辑在拷贝前比较目标的 `size + mtime`，一致就跳过。XFS 的 mtime 精度受时钟粒度限制，同一 tick 内把文件内容等长改写两次，源与目标的 `size + mtime` 可以完全一致 —— 事件已经到了，却被 `stat` 否证，目标端**永久停在旧内容**。NTFS 是 100ns 精度，所以开发机上从来没复现。

修法：收到 `Write` / `Create` 事件就无条件拷贝；只有 `Attrib`（仅属性变化）才走 `size + mtime` 快路径。注释原文：

> A write or create event is proof the content changed, so it is copied unconditionally. Do not "verify" it with size and mtime: filesystems cap timestamp resolution (1 ms on XFS here, 1 s on ext3, 2 s on FAT), and an application that writes twice inside one tick leaves the source looking identical to the stale copy on the target.

回归测试：`TestSameTickWriteStillCopies`（断言必须重拷）、`TestAttribOnlySkipsIdenticalFile`（断言快路径没被误伤）。

#### 记录下来的路径不等于还活着的 inode（`watcher` 的 `AddWatch`）

`rm -rf x && cp -a y x` 是镜像场景里最普通的操作，但它会让 `Tree.dirs` 与 `inotifyBackend.dirToWd` 里的 `x` 指向一个**已经被内核回收的 inode**。旧实现在 `addOne` 里 `if _, ok := t.dirs[p]; ok { return }` 早退，于是同名目录重建后**既不装新 watch 也不回填**，整棵子树永久失聪直到下一次全量重同步。

真机现象很能说明问题：burst 场景跑完后 `kill -QUIT` 抓到 22 个 goroutine **全部空闲**（不是死锁、没有阻塞在满通道上），日志停在 `entering main loop` 之后再无一行 —— 内核根本没再送事件进来。

修法：

- 新增 `inotifyBackend.AddWatch(path) (fresh bool, err error)`：**无条件**调一次 `InotifyAddWatch`（对同一 inode 幂等），把返回的 wd 与表里的旧值比较，不同就是新 inode，返回 `fresh = true` 触发回填；
- `Tree.watchOne` 一律先问后端再记表。注释原文：「The recorded set is bookkeeping, never proof. A path can outlive its inode … Only the backend can answer that, so the decision is delegated to it.」

代价是每个目录多一次 syscall（仅目录创建时，不在热路径上），换来的是「删除后同名重建」这类镜像日常操作不再需要靠 full_resync 兜底。

#### `IN_DELETE_SELF` 不带 `IN_ISDIR`，而 `IN_IGNORED` 要清表

内核对 `IN_DELETE_SELF` / `IN_MOVE_SELF` **不设 `IN_ISDIR`**（ISDIR 描述的是「父目录里那个条目」）。而 `Tree.handle` 靠 `ev.IsDir && ev.Op&Delete` 才清 `t.dirs`，于是被删目录的条目永远残留 —— 正是上一条的触发条件。三处修正：

- `IN_DELETE_SELF` 分支显式 `isSelf = true`（我们只 watch 目录，self 事件的主语必然是目录）；
- `IN_MOVE_SELF` **不产生事件**：裸的 MOVED_SELF 没有目的路径，转成对旧路径的 Delete 会删掉一个只是改了名的目录镜像；真正的改名由父目录的 MOVED_FROM/MOVED_TO 配对上报；
- `IN_IGNORED` 且不是 self 事件时，先清 `wdToDir[wd]` 与 `dirToWd[dir]` 再返回 —— 否则每删一个目录就漏一条死 wd 条目（issue #686 那类「内存只涨不落」的缓慢泄漏）。

#### 快照后端不是「每目录一个 watch」（`watcher` 的 `PerDirectory`）

`scanTree` 对每个子目录调 `addOne` → `be.Add(subdir)`，而 `poll` 后端把每次 `Add` 都塞进 `roots`。于是在 Windows / macOS 上，**每个 tick 会把同一棵树按目录数重复 walk 一遍** —— 1 万个目录 = 每个 tick 走 1 万遍全树，纯粹的二次方，和「轮询只是慢一点」的直觉完全相反。

修法：新增可选接口 `perDirectoryBackend { PerDirectory() bool }`，`poll` 后端返回 `false`；`AddRoot` 对非逐目录后端只登记根，不再递归登记子目录，`handle` 的新目录分支也不再投递给 adopt 池。未知后端默认按 `true` 处理，保持旧行为。

同一轮还修了 `event.Op.String()`：回填合成的是 `Create|Write`，旧实现落到默认分支打印 `Op(?)` —— 恰好在追查丢文件时丢掉最关键的信息。

#### rsync 后端：重命名与目录删除的三个语义坑（`sync/rsync.go`）

`direct` 后端在进程内 `rename(2)`，所以这些问题在 Windows 开发机上看不出来；换成 `mode = "rsync"` / `"rsyncssh"` 并对着真实 rsync 3.1.2 跑，才逐条暴露：

1. **重命名只传新名字，旧名字永不删除**。一个 `mv` 在引擎里是一条 `Kind=Move, Path=新, From=旧` 的变更，旧实现只把 `Path` 塞进 `--files-from`，于是目标端同时留着新旧两份，每次 rename 都让镜像膨胀一点。修法：`plan()` 在见到 `Move` 时把 `From` 一并排进删除列表。
2. **`--delete-missing-args` 删不掉非空目录，而且退出码还是 0**。实测输出 `cannot delete non-empty directory: keep`，子树原样保留，`rsync` 返回成功 —— 这是最危险的一种失败：镜像悄悄漂移，日志里没有任何错误。修法：`deleteArgs()` 里加 `--force`，让列出的目录连同内容一起被移除，与本地目标 `os.RemoveAll` 的行为对齐。
3. **`--files-from` 会抑制 `-a` 隐含的 `-r`**。rsync 手册明写「The only exception … is when `--files-from` is specified， in which case `-r` is not implied」，所以 `baseArgs()` 里 `-r` 是显式写死的。少了它，运行期新建的目录会被创建但**内容一个都不会传**，线上表现是「目录出现了，里面是空的」。

三条都有单测钉住：`TestRenameSchedulesOldNameForDeletion`、`TestRemoteDeleteForcesNonEmptyDirectories`、`TestIncrementalArgsNeverDelete`（内含 `-r` 断言）。第二条另外用一台 loopback `rsyncssh` 目标做了端到端验证：rename 后 `dst/keep` 消失、`dst/keep2/{k1.txt,inner/k2.txt}` 内容为新值、日志里 `cannot delete non-empty` 计数为 0。

**顺带一条 `--files-from` 的事实**（不是 bug，但配置压测时会误判）：列表里写 `d` 只创建空目录 `d`，写 `d/` 才会递归内容。gsyncd 传的是不带尾斜杠的路径，内容靠随后的逐文件事件补齐 —— 因为目录本身出现在变更里时，内核必然已经为它产生了子条目的 Create 事件。

### 3.4 故障注入：生产环境里真正会发生的九类事故

单测和 e2e 覆盖的是「逻辑对不对」，覆盖不了「环境坏了会怎样」。下面这些组都是**真机注入故障**跑出来的，每组都同时看两件事：镜像最终是否一致，以及进程在故障期间是否保持克制（CPU、内存、fd、日志）。

#### 链路断掉（issue #250）

把 `rsyncssh` 目标指到一个没人监听的端口，让它连不上，跑满 60 秒再数 CPU tick：

| 指标 | 结果 |
|---|---|
| CPU | 25 tick / 60s ≈ **0.4%**（原版 issue #250 是 100% 忙等） |
| RSS / fd / 线程 | 10MB / 7 / 12 |
| 日志 | 11 行，其中重试相关 1 行 |
| 行为 | `initial sync failed after 15ms` → `entering main loop` → 之后完全静默 |

原因有两层：`sync.Backoff` 让重试间隔从 15s 指数上升到 10 分钟封顶，而 `Root.retryAt` 这道门让「失败的整轮」在到期前根本不重新发起 —— 没有这两层，断链的机器会以 delay 的频率反复 `rsync` 全树，把源端磁盘先打满。

更贴近真实的一组是把链路做成可开关的（loopback 上起一个私有 sshd 当「网络」），先同步 100 个文件，断链，期间写入 400 个文件，等 90 秒，恢复链路：

| 指标 | 结果 |
|---|---|
| 断链 90 秒 CPU | 37 tick / 90s ≈ **0.4%** |
| 断链期间是否丢事件 | **否**，400 条全部留在聚合窗口里 |
| 恢复后是否需要人工干预 | 否，`converged after ~39s with 500 files` |
| 结束时源/目标文件数 | 500 / 500 |

最后一条是关键：**网络故障不是数据问题，只是延迟问题**。事件在 `delay` 窗口里累积，窗口不会被丢弃，失败批次回填后下一轮重传（`readd`），所以恢复链路后镜像自己就补齐了。

#### 内核事件队列溢出

`fs.inotify.max_queued_events` 临时压到 512（远小于一次 20 万文件的突发），用 16 路并发写入把队列撑爆（7559 文件/秒）：

| 指标 | 结果 |
|---|---|
| 收敛 | `CONVERGED=yes after 75s`，源 200001 = 目标 200001 |
| 全量重同步次数 | `full=2 overflow=2` |
| `watched_dirs` | 3（没有因为重扫而泄漏） |
| RSS / 线程 / fd | 83MB / 21 / 7 |
| 日志 | **11 行 / 808 字节** |

原版在这里是 `runner.overflow` → 直接 `hup` 重启进程（`lsyncd.c` 的 `main1` 用 `exit(-1)`，不做任何清理），重启期间事件全丢，10T 树上重启代价是小时级。gsyncd 只把窗口标成 overflow，下一次 flush 提交一个 `Batch{Full:true}` 让后端整体重同步一次，进程不动、watch 不重建。

顺带修掉两个只有这一组会暴露的问题：

1. **溢出警告刷屏**。内核每丢一批事件就置一次 `IN_Q_OVERFLOW`，突发期间三秒内打了 19 行一模一样的 `watcher queue overflowed` —— 这正好是 issue #93 的成因（单机 438GB 日志）。改成 `logx.Throttlef("overflow:"+name, time.Minute, ...)`，一分钟一行，被抑制的条数附在消息尾部。
2. **`overflow` 计数永远是 0**。引擎 `Stats.Overflows` 从来没被递增过 —— 状态文件里的 `overflow=` 字段形同虚设，而它恰恰是运维判断「这个镜像是否已经重建过」的唯一口径。现在按**事件段**计数（`r.agg.Overflowed()` 为假时才加一），两次突发算两次，一次突发里的 19 个事件算一次。

#### 整个源根目录消失

这是本项目最危险的一个缺陷，而且它不是并发问题，是一个被吞掉的错误。

`walkCopy` 用 `filepath.Walk(d.Source, ...)` 遍历，回调第一行是 `if err != nil { return nil }` —— 这是为了容忍扫描过程中某个子树被删掉（否则一次 `rm -rf` 就会让整轮全量同步失败）。但 `filepath.Walk` 在**根路径本身不存在**时也只回调一次 error，同样被吞掉，于是整轮全量同步变成 no-op，而引擎照样打 `initial sync done`。

真机复现（2 万目录 / 10 万文件，启动后 `rm -rf /root/E/src`）：

```
INITIAL_DONE_AFTER=15
DST_FILES_BEFORE=100000
RM_SECONDS=22
… 300 秒轮询期间 dst_files 一直是 100000 …
status: pending=0 errors=0 full_syncs=1 overflows=1
```

也就是说：**源没了，目标端 10 万个文件一个都没删，日志里没有任何错误**。overflow 触发的全量重同步走 `Apply → b.Full → Startup → walkCopy`，正好是这条被吞掉的路径，所以它连「重同步过了但什么都没做」都看不出来。

修法是 `walkCopy` 开头先确认根目录可读：

```go
if _, err := os.Stat(d.Source); err != nil {
    return fmt.Errorf("source root is not usable, refusing to sync: %w", err)
}
```

两个方向都不能选，这是本条的关键取舍：

- 把「不存在」当成「空」→ `pruneDir` 会**删光整个目标端**。未挂载的文件系统、写错的 `source`、权限掉了的 NFS，任何一种都会让镜像被清空，而这比不同步严重得多。
- 继续吞掉错误 → 就是上面那次实测：目标端静默过期，运维以为一切正常。

所以**拒绝同步**是唯一安全的答案：目标端保持原样，日志每分钟一行 `transfer failed … refusing to sync`，状态文件 `errors` 递增，`full_syncs` 也照样递增（证明它确实尝试过）。同一份修复后的实测：

```
t=45s dst_files=100000            ← 目标端没被清空
Warning: [e] watcher queue overflowed, scheduling a full resync
Warning: [e] window overflowed, forcing a full resync
Warning: [e] retry 1 in 1.002s: source root is not usable, refusing to sync: stat /root/E/src: no such file or directory
Error:   [e] transfer failed after 3 attempts: source root is not usable, refusing to sync: …
```

回归测试 `TestStartupRefusesMissingSourceRoot` 与 `TestFullBatchRefusesMissingSourceRoot` 分别钉住「启动时」和「overflow 自动重同步时」两条入口，后者是无人值守的那条。

#### 目录在监视中消失又重建

比上面轻一层、但更常见的是目录**重建**：`rm -rf x && cp -a y x` 是镜像场景里的日常操作，但它会让 `inotify` watch 指向一个已回收的 inode。旧实现信任自己的 `dirs` 表，看到路径还在就当「已经 watch 过」直接返回 —— 于是新子树**永远没有 watch，也不做回填**，日志停在 `entering main loop` 之后一片安静，而目标端差异一路涨到 19995 个文件。修法见上面 §3.3 的第二条：watch 表只是记账，唯一能回答「这个 inode 还在吗」的是后端（`AddWatch` 无条件重挂并比对 wd）。Linux 专用测试 `TestRecreatedSubtreeStillReportsEvents` 钉住这条。

#### 源根本身消失后重建：watch 自动重挂

上一节讲的是「子树被删重建」，后端靠 `AddWatch` 无条件重挂 + 回填修好了。但**根目录自己**被删重建是另一条路径，而且更隐蔽：

- 根被 `rm -rf` 时，内核发 `IN_DELETE_SELF`，随后发 `IN_IGNORED`。原实现的 `dispatch` 在 `IN_IGNORED` 分支里只做一件事——清 `wdToDir` / `dirToWd` 防泄漏，然后 `return`，**一个事件都不发**。引擎完全不知道这棵树没了。
- `Tree.handle` 收到目录删除只做 `delete(t.dirs, path)` + `forgetBelow`，而 `t.roots` 里那条记录仍在；重建根目录时，根的父目录不在 watch 集合里，所以**不会有任何 `Create` 事件**触发 `adoptTree`。
- `main` 里的 `tree.AddRoot` 只在启动时执行一次。

净效果：该 root 永久失明，只能靠 `full_resync` 兜底；而 `unwatched == 0` 时 `main` 根本不会自动启用 `full_resync`（§3.2 的「watch 表打满」那条逻辑）。

真机复现（2 万目录 / 10 万文件，`rm -rf src` 后用同样形状重建）：

```
DST_AFTER_RM=100000        ← 拒绝同步生效,目标端没被清空
REFUSALS=2  EVENTS_AFTER_RM=63161
重建后 src=100000 dst=100000 → RECOVERED_CONTENT=yes   ← 假象:两侧数量恰好相同
mkdir src/brandnew; echo x > src/brandnew/probe.txt
轮询 120 秒 → NEW_FILE_MIRRORED=no                     ← 真缺陷:失明
status: watched_dirs=12326 unwatched_dirs=0 pending=0 errors=1
```

`RECOVERED_CONTENT=yes` 是陷阱：重建的树和原来的树文件数相同，只是「看起来一致」，期间一个字节都没传。

修法分四层，前三层让「根没了」这件事变得可见，第四层负责在通知本身失效时兜底。

1. **后端上报**。`inotify` 的 `dispatch` 在 `IN_IGNORED` 分支清表**之前**先用 `rootOf(wd)` 判断这个 wd 是不是某个 sync root，是就补发一条 `event.RootGone`（Path 就是该 root）。`poll` 后端在 `scan` 里对 `Walk` 回调的 `p == root` 错误做同样的事。`RootGone` 是独立 Op 而不是复用 `Overflow`：它只点名一棵树，不需要其他 sync 陪跑全量。
2. **watcher 记账**。`Tree` 增加 `lost map[string]bool`：`handle` 收到 `RootGone` 时把该 root 标进 `lost`、从 `dirs` 删掉并 `forgetBelow`。`AddRoot` 的短路条件从 `t.roots[root]` 改成 `t.roots[root] && !t.lost[root]`，否则「已知」会被误当成「还在 watch」。新增 `IsWatched` / `Lost` / `Reattach`。
3. **main 周期巡检**。`superviseRoots` 每 `rootCheckInterval = 10s` 对每个 sync 做 `tree.Verify` + `tree.IsWatched`：目录回来了但没在 watch 里，就 `tree.Reattach` 然后 `eng.Reattach`。`eng.Reattach` 清 `lost` / `fails` / `retryAt`（根缺失导致的失败不代表目标不可达），`MarkOverflow()` 强制一次全量，`flushAsync` 立即提交——不等 delay，因为这段窗口里树本来就是盲的。
4. **inode 身份校验**。`Tree.rootIdent map[string]fileIdentity` 在 `AddRoot` 时记下根的 `(dev, ino)`（Windows 上是卷序列号 + `FileIndex`），`Verify(root)` 只读一次 `stat` 与之比对。这是唯一能抓住 `rm -rf data && cp -a new data` 的信号：路径没变、目录还在、`os.Stat` 成功，只有 inode 换了。`Reattach` 因此也重写了——只要身份对不上，就不能信任 `dirs` 里那批子目录，必须 `takeBelowLocked` 清掉再完整重扫，否则 walk 会认为「已经 watch 过」而跳过整棵树。

**为什么第三层必需**：内核丢 watch 的方式不止 `IN_IGNORED` 一种（卸载、`fanotify` 迁移、watch 表被裁剪），而**重建出来的根目录没有任何事件能告诉任何人它回来了**。只有周期性 `stat` + 记账比对能兜住。

**为什么第四层必需**（这一条是实测打出来的，不是推演）：2 万目录 `rm -rf` 时 `IN_Q_OVERFLOW` 会**抢在** `IN_IGNORED` 之前到达，`dispatch` 里 `wdToDir` 已被溢出路径清空，随后那些 `IN_IGNORED` 全在 `if !ok { return }` 处静默丢弃——包括根自己那条。实测 `REFUSALS=0 ROOTGONE_LINES=0 root_gone=0`，即内核通知一条都没送到，而 inode 校验在同一场景下把树重新接上了。**结论：内核通知只能当加速器，不能当证据。**

修复后真机复测（同样的 2 万目录 / 10 万文件，`/root/mrec5.sh`）：

```
MAIN_LOOP_AFTER=12   DST_BEFORE=100000
rm -rf src  → DST_AFTER_RM=100000  REFUSALS=0  ROOTGONE_LINES=0   ← 内核什么都没报
重建 src    → marker mirrored after 20s   MARKER_MIRRORED=yes  REATTACH_LINES=1
新目录+新文件 → new file mirrored after 5s   NEW_FILE_MIRRORED=yes
改已有文件    → modification mirrored after 5s  MOD_MIRRORED=yes
删树内文件    → deletion mirrored after 10s    DELETE_MIRRORED=yes
SRC_FILES=100001  DST_FILES=100001
```

日志里对应的三行是 `re-attached /root/H/src after losing its watch, forcing a full resync` → `applied 707 changes in 23.624s` → `applied 29949 changes in 2.963s`；状态文件 `lost_roots` 回到空，`watched_dirs` 从 0 恢复到 22003。**注意 `root_gone=0` 与 `MARKER_MIRRORED=yes` 并存**——这正是第四层在单独起作用的证据。

观测面：状态文件新增 `lost_roots`（数组）、每 sync 的 `root_gone`（计数）与 `root_lost`（当前是否盲），`-status` 会直接打 `warning: watch lost on /path (the tree is not being observed)`。这条警告是必要的：失明期间进程健康、计数不动，和空闲系统在日志上无法区分。

回归测试：`TestRootGoneMarksRootLost`、`TestReattachRewatchesTree`、`TestVerifyDetectsRecreatedRoot`、`TestReattachReplacesStaleChildren`（watcher），`TestRootRemovalReportsRootGone`（linux，真内核删根），`TestRootGoneMarksSyncLost`、`TestReattachForcesFullResync`、`TestReattachClearsRetryGate`、`TestReattachIgnoresUnknownSource`（engine）。


#### 磁盘写满（ENOSPC）

目标端在一轮同步中途写满，是最容易把「静默漂移」变成常态的一种故障：前面已经拷好的文件是对的，后面每个文件都失败，而失败一旦被吞掉，镜像就永久停在半个状态。

真机注入（源 40 × 1 MiB，target 挂一个 `size=32m` 的 tmpfs）：

| 观察项 | 结果 |
|---|---|
| 进程 | 存活，`entering main loop` 之后照常运行 |
| CPU | 10 秒内 1 tick，没有忙等重试 |
| RSS | 5.2 MB |
| 日志 | `Error: initial sync failed after 15ms: write …/.gsyncd-tmp-…: no space left on device` 两行，不刷屏 |
| 临时文件残留 | **0**（`copyFile` 的 `defer` 把 staging 文件删掉了） |

真正的问题在第一版里暴露出来：**初始同步失败之后什么都不会发生**。失败的文件在源端已经存在、不会再有事件，于是永远没有东西去修它 —— 把盘扩到 128 MiB 之后 `touch` 一个文件，只有那一个文件被补拷，其余 39 个永久缺失。

修法（`engine.Startup` 的失败分支）：初始同步失败时，除了记 `Errors` / `LastError`，还要 `fails++`、按 `sync.Backoff` 设 `retryAt`、并 `MarkOverflow()` —— 让引擎挂起一次全量重同步，走和运行期失败同一条退避门。这样盘满、未挂载、权限这类**外部原因消失后它会自己收敛**，不需要人工重启。

回归测试：`TestFailedStartupArmsFullResync`（断言 `Startup` 返回错误后 `Overflowed()` 为真、`fails/Errors` 各加一，`retryAt` 到期后恰好提交一个 `Full` 批次）。

#### 内存被 cgroup 限死

共置场景下 gsyncd 经常跑在别人的 cgroup 里，内存上限可能只有几十 MB。这一组测的是「被限死时是变慢还是被杀」：

源 200 × 256 KB + 200 个目录 × 50 个小文件（共 10200 个文件），`/sys/fs/cgroup/memory/…/memory.limit_in_bytes = 48 MiB`，启动后把 pid 写进 `cgroup.procs`:

| 指标 | 结果 |
|---|---|
| `memory.usage_in_bytes` | 24.0 MB |
| `max_usage_in_bytes` | 24.1 MB（峰值贴着上限之下） |
| `memory.failcnt` / `oom_kill` / `under_oom` | 0 / 0 / 0 |
| 行为 | 只是变慢；30 秒时已同步 1831/10200 |
| 抬到 256 MiB 后 | 52 秒内收敛，10200 / 10200，内容逐文件 `sha256` 全一致 |

结论：48 MiB 硬上限下峰值只用了 24.1 MB，**没有 OOM、没有 failcnt、没有降级**，代价只是慢。这也印证了 §3.5 末尾那句「RSS 与目录数线性、不随事件数增长」：窗口、通道、子进程输出、map、日志五处都有上限，所以内存不会因为事件洪峰而爆。

#### 系统时钟被大幅调整

镜像机器上 NTP 跳变、运维手改 `date`、虚拟机从快照恢复，都会让墙上时钟前后跳几小时甚至几十天。若超时判定用的是 `time.Now()` 的差值，这类跳变会让 `delay` 永远不到期（时钟往回跳）或者立刻到期（往前跳）。

真机注入（停 `chronyd`，delay=2s）：写 `v1` → `date -s '+2 hours'` 写 `v2` → `date -s '-2 hours'` 写 `v3` → `date -s '-30 days'` 写 `v4` → 用 `/proc/uptime` 差值把时钟校回 → 写 `v5`。

结果：**五次全部跟上**，CPU 8 tick，`errors=0`。原因在实现里：`engine` 的超时全部基于 `time.Time` 的单调时钟读数（`time.Since` / `now.Sub`），`status` 文件里的时间戳只是给人看的；`sync.Backoff` 的退避也走同一套。回归上没有专门的用例 —— 这类行为只能靠真实内核改时钟验证，单测里注入的假时钟反而测不到。

#### 文件被长时间占用写（issue #77 / #458）

这一条是四个补测场景里唯一**测出了真缺陷**的。一个文件被进程持有并持续追加时（`exec 3>big.bin` 然后每秒写 1 MiB），它在 `close` 之前不会有 `IN_CLOSE_WRITE`，而旧的 mask 里根本没有 `IN_MODIFY`：

独立探针（只用 gsyncd 的原始 mask 订阅同一个目录）在写入持续 8 秒的那一轮里，完整输出是 —— `IN_CREATE` 一条，然后**从开始计 12 秒后**才收到 `IN_CLOSE_WRITE` 一条。**中间什么都没有。**

端到端表现：写 10 MB 的 24 秒里，目标端从 t=3s 起恒为 2 MB，直到 close 后才跳到 10 MB。也就是说写入期间镜像一直是旧快照，而这正是 issue #77「copy before write」与 #458 描述的场景。

修法（三件套，缺一不可）：

1. **mask 加 `IN_MODIFY`**，`dispatch` 里映射成 `Write`。连续 `IN_MODIFY` 由聚合器折叠成同一条 pending change，所以流式 4 KiB 写每笔只花一次 map 更新，不会放大成每写一次拷一次。
2. **`max_hold` 上限**（默认 1h，键名也接受 `maxhold`，负值关闭）：`Aggregator` 记 `oldest`（窗口内最早被触碰的时间），`Tick` / `flushAsync` 除了「静默 `delay`」之外，再加一条「最老的 pending 已经等了 `max_hold`」就强制 flush。这样永不静默的文件也能被周期性地追上。
3. **`copyFile` 改成有界拷贝**：`copyBounded(tmp, in, info.Size())`（0.1.15 起用池化的 1 MiB 缓冲，此前是 `io.CopyN` 的 32 KiB）。无界 `io.Copy` 会把仍在写入的字节也拉进来，却又盖上调用方 `stat` 到的那个 mtime —— 下一轮就可能被误判成「已最新」。短读只在文件变小时发生，无害。

真机复测（`max_hold=6`、`delay=2`，一个文件被持有并每秒追加 256 KiB 共 30 秒，合计 7.5 MiB）：

```
t=7s  dst=1572864     t=15s dst=3145728     t=24s dst=4718592
t=32s dst=6160384     t=39s dst=7602176
writer 关闭后 60 秒:src=7864320  dst=7864320  sha256 一致(CONTENT_OK)
applied 6 次,staging 残留 0,CPU 37 tick,errors 0
```

即每约 8 秒（`max_hold` + `delay` + 一次拷贝）追一次，不再冻结到 close。同一台机器上先做过一次未加 `IN_MODIFY` 的对照：40 秒写入期间目标端只更新到 18 MB / 41 MB 就再也不动，最终 `CONTENT_MISMATCH` —— 差别就在这一位掩码与 `max_hold`。代价要写清楚：这类文件在写入期间镜像总是落后最多 `max_hold + delay + 一次拷贝`，而且每次强制拷贝都从文件头读起 —— 对 10 GB 级持续被写的文件是明显的读放大。要更贴近就调小 `max_hold`，要更省 IO 就调大，或者直接 `excludes` 掉让数据库自己的备份去处理。

回归测试：`TestOverdueFiresOnOldestPending`、`TestOverdueDisabledAndEmpty`、`TestOverdueResetsOnFlush`（aggregate），`TestMaxHoldFlushesAFileThatIsNeverQuiet`、`TestMaxHoldDefaultsAndOptOut`（engine）。

#### 等到关闭再同步（`close_write`，issue #458）

上一节的修法让「一直被写」的文件能被周期性地追上，但每一份快照都取自写入中途：目标端上的那份文件在 writer 关闭之前，大小总是落后一截，内容也总是半截。对日志和 WAL 这没问题（消费者本来就只读到某个偏移），对**数据库数据文件、虚拟机镜像、被 mmap 的索引**就未必 —— 目标端看起来完整，实际是一个撕裂的快照，而 rsync 下一次又会因为 size+mtime 与源一致而跳过它。

`close_write = true` 就是把「一次写入结束了没有」的判定从「事件静默了」换成「writer 关闭了」。

```
[[sync]]
source      = "/srv/pg"
target      = "/backup/pg"
delay       = 5
max_hold    = 300          # 必须留着
close_write = true
```

实现是三处小改动，难点全在「哪个事件才算关闭」：

1. **内核层**：`IN_CLOSE_WRITE` 与 `IN_MODIFY` 在 `dispatch` 里本来都映射成同一个 `event.Write` 位，引擎无法区分。新增独立的 `event.Close` 位（第 8 位），`IN_CLOSE_WRITE` 置 `Write|Close`，`IN_MODIFY` 仍然只置 `Write`。
2. **聚合层**：`Change` 加未导出的 `open` 标志，`Aggregator` 增量维护 `openFiles` 计数。**不能从 `Op` 掩码推导**：掩码是整个窗口累积出来的，「关闭后又写一次」的条目会同时带 `Write` 和 `Close`，只有最新那个事件能回答「现在写完没有」。计数必须是增量的，`Tick` 每 200ms 问一次，遍历窗口会变成 O(window)。
3. **引擎层**：`Root.ready` 拆成「强制条件」与「安静条件」。强制条件（`Overflowed` / `Urgent` / `Overdue`）不受这个开关影响 —— 丢了事件或者撞上 `max_hold` 时，镜像是已知不正确的，再等 close 只会让错误状态持续更久。安静条件在 `now.Sub(last) >= Delay` 之后多看一条 `CloseWrite && agg.OpenFiles() > 0`。

四条容易被写错、已经各有用例钉住的语义：

- **`delay` 仍然表示「安静这么久」**，close 只是把计时重置，不是立即刷。所以一个写完就关的文件行为与不开这个开关时完全一样（仍然只拷一次），只是计时起点从最后一次写挪到了 close。
- **单独的 `Create` 不算「打开」**。create 事件也可能是 `ln -s`、fifo、设备节点、或者 rename 落位 —— 这些永远不会收到 `IN_CLOSE_WRITE`，把它们算作打开会让一条 `ln -s` 把整个窗口卡到 `max_hold`。
- **rename 不关闭文件**。移动条目时 `open` 标志跟着走（`applyMove` 里取出再带回），否则「写入期间重命名」会正好触发那一次半截拷贝。
- **重试不重新判定**。批次取出后 transfer 失败要 `readd` 回窗口，如果按 `Op` 掩码重建事件，`Write` 无 `Close` 会被重新读成「打开」，把已经到达的 close 撤销 —— 在 `close_write` 下这次重试要等到 `max_hold`。所以走 `Aggregator.Readd`，保留条目原有状态。

**`max_hold` 是这个开关的前提，不能关**。一个文件被打开一次然后永远不关（数据库 datafile、mmap 文件、被长驻进程持有的句柄）在事件层面与「正在被写」无法区分 —— 两者都是「还没收到 close」。若允许关掉 `max_hold`，`close_write` 就变成「这类文件永远不同步」，而且 `pending=0`、`errors=0`、`phase=running` 全绿、没有任何日志。全绿但数据错，比报错危险。所以 `close_write = true` 与 `max_hold < 0` 的组合在 `config.Validate` 与 `engine.New` 两处都被拒绝。

三种组合的实际风险：

| 配置 | 半截文件 | 陈旧上限 | 永久不同步 |
|---|---|---|---|
| 默认（`close_write` 关） | 可能（写入期间每次快照都是半截） | `delay` | 不会 |
| `close_write = true` + `max_hold` | 基本消除（仅 `max_hold` 强刷那一次是半截） | `max_hold` | 不会 |
| `close_write = true` 且关 `max_hold` | 消除 | 无 | **会**（被拒绝） |

真机实测（0.1.21,8 核 / 62G，目标端同一块 LVM 卷）。写者每秒 append 256KiB 共 20 秒，**写完再保持 fd 打开 20 秒**才 close，`delay = 2`:

| 配置 | 写入期间 | close 之后 | 最终 |
|---|---|---|---|
| `close_write = true`，`max_hold = 300` | 目标端**连文件都不存在**（40 秒内 `open=1 pending=1 files=0`） | `t+4s` 落盘 5,242,880 字节 | sha256 一致 |
| 默认（关），`max_hold = 300` | `open=1` 只是信息，不阻塞；`t+24s` 已落盘 | —— | sha256 一致 |
| `close_write = true`，`max_hold = 8` | 每 8 秒被 `max_hold` 强刷一次（2,097,152 → 4,456,448） | 最后一次补齐 | sha256 一致 |

最直接的对照是让写者写完 2MiB 后**暂停 12 秒**（fd 仍打开），暂停期已超过 `delay`：

| 配置 | 暂停期间目标端 | close 之后 |
|---|---|---|
| 默认 | 已经躺着半截的 2MiB(`t+3s dst=2097152 files=1`) | 补齐到 4MiB，sha256 一致 |
| `close_write = true` | 目标端不存在该文件 | 一次写入 4MiB，sha256 一致 |

也就是说默认配置下那半截快照是**真的会落到目标端并被读取方看到**，不是理论风险；开了这个开关它就不存在。

**已知的两处粗糙**，都记在这里而不是当成功：

- `Overdue` 判的是整个窗口最老的变更（`aggregate.go` 的 `a.oldest`），不是按路径。一个长驻写入方会把同窗口里其他文件一起拖到 `max_hold`。后果只是批次节奏变粗（内容仍然正确），默认 `max_hold = 1h` 下也很少命中；真正按路径判需要在最热路径（`appendItem`）上再加一套最小堆加惰性删除，而正确解法是拆 sync —— 给常写目录单独一条 `[[sync]]`（`wal: delay=2 max_hold=60` / `data: delay=30 max_hold=3600`），引擎按最长前缀路由，嵌套 root 下事件只进最长前缀那条。
- 非 Linux 的轮询后端（`internal/watcher/poll_other.go`）是快照 diff，只能看出「文件变了」，永远发不出 close。最初的做法是让它静默退化（每个 `Write` 都算「打开」，只有 `max_hold` 能刷）—— 但运维从日志里看不出自己拿到的是「等写完」还是「等 max_hold」，所以改成**启动时直接拒绝**：`watcher.CloseReporter` 接口由后端声明 `ReportsClose()`，`checkCloseWrite` 在装了 `close_write` 而该后端返回 `false` 时让进程带着明确的错误退出。本项目的目标平台是 Linux，这个限制只影响 macOS/Windows 上的调试构建。

回归测试：`TestWriteWithoutCloseCountsAsOpen`、`TestCloseClearsOpen`、`TestWriteAfterCloseReopens`、`TestAttribDoesNotChangeOpenState`、`TestDirectoriesAreNeverOpen`、`TestCreateAloneIsNotOpen`、`TestBackfillMaskIsNotOpen`、`TestDeleteClearsOpen`、`TestRenameKeepsOpen`、`TestDirDeleteReleasesOpenChildren`、`TestFlushResetsOpenCount`、`TestReaddKeepsClosedState`、`TestReaddKeepsOpenState`、`TestRenameOntoPendingPathStaysOpen`、`TestOpenCountStaysConsistent`、`TestFlushClearsOpenCountWithoutStrandingIt`（aggregate）；`TestCloseWriteHoldsFlushWhileFileIsOpen`、`TestCloseWriteWaitsForEveryOpenFile`、`TestCloseWriteStillWaitsForDelayAfterClose`、`TestCloseWriteMaxHoldForcesFlush`、`TestCloseWriteDoesNotHoldBackOverflow`、`TestCloseWriteIsOffByDefault`、`TestCloseWriteRejectsDisabledMaxHold`、`TestCloseWriteIgnoresDirectories`、`TestCloseWriteDoesNotBlockShutdown`（engine）；`TestCloseWriteKey`、`TestCloseWriteRejectsDisabledMaxHold`（config）；`TestCloseWriteCarriesItsOwnBit`（watcher，真实内核）；`TestCheckCloseWriteAcceptsReportingBackend`、`TestCheckCloseWriteRejectsSnapshotBackend`、`TestCheckCloseWriteAllowsSnapshotBackendWithoutOption`、`TestCheckCloseWriteAcceptsBackendWithoutInterface`（cmd/gsyncd，后端能力守卫）。

### 3.5 生产隐患逐项对照

把「生产上会怎么坏」逐条列出来，每条指明防在哪个文件、靠什么机制，以及它是被测试还是被真机故障注入验证过。这张表是审计的入口：任何一条没有对应机制，就是一个待补的口子。

| 隐患 | 表现 | 防线 | 验证 |
|---|---|---|---|
| 目标端宕机 → 重试风暴 | 每 delay 一轮全树 rsync，CPU 与源盘打满（issue #250） | `engine.Root.retryAt` + `fails`，退避 15s→10min；`sync.Backoff` 带抖动 | 断链 60s/90s 实测 0.4% CPU |
| 失败批次被静默丢弃 | 镜像永远差一批文件，日志无错 | `apply()` 失败后 `readd()` 把变更放回窗口；失败的全量重传重新 `MarkOverflow()` | `TestFailedBatchIsReadded`、`TestFailedFullResyncStaysFull` |
| 内核事件队列溢出 | 事件丢失，镜像静默过期 | `IN_Q_OVERFLOW` → `Overflow` 事件 → `Batch{Full:true}` 整体重同步，进程不重启 | 20 万文件突压实测收敛 |
| 用户态通道比内核队列小 | 溢出未被察觉就丢事件 | `inotify` out 32768、`Tree.out` 65536，均大于 `max_queued_events` 默认 16384 | 代码约束 + 溢出实测 |
| 聚合窗口无上限 | 百万事件时 OOM | `max_pending`（默认 50 万）达上限即**提前 flush**，不丢事件 | `TestSizeCapIsUrgentNotOverflow` |
| 日志写爆磁盘 | 单机 438GB 日志（issue #93） | `logx.Throttlef` 全链路节流 + 64KB `bufio` + 轮转 + 进度日志每 5 万条一行 | 溢出/断链实测均 11 行日志 |
| 子进程输出撑爆内存 | rsync 进度条不打换行 → 缓冲无限增长 | `splitLines` 按 64KB 强制切段；非逐行模式用 `cappedBuffer` 只留尾部 16KB | `TestSplitLinesCapsUnterminatedLine`、`TestCappedBufferKeepsTail` |
| 取消后孤儿 ssh/rsync | `exec.CommandContext` 只杀直接子进程 | `setProcGroup` + `cmd.Cancel = killGroup`，整组信号 | 平台相关，`proc_unix.go` / `proc_windows.go` |
| 并行失败被吞 | 一批里只有一条报错，其余静默失败 | `Apply` 用 `firstErr` + `note()` 累计，`Stats().Errors` 逐条加 | `TestApplyReportsAllFailures`（8 条全失败 → Errors=8） |
| 同一目标两个写者 | 一个在拷、一个在删，镜像变成混合状态 | `Run` 收尾先 `wait` 再 `FlushAll` 再 `wait`；`flushSync` 自旋等 `flushing` 落 | 代码顺序约束 |
| 两个实例同时启动 | 同上，且日志毫无线索 | pidfile 活性检测（`pidAlive`），活着就拒绝启动 | `TestWritePidFileRejectsLiveInstance` |
| `.gsyncd-tmp-*` 残留堆积 | 断电后 stale 临时文件永久占空间 | `sweepTemp` 在每目录启动拷贝时清理 24h 以上的非目录临时文件 | `TestSweepTempRemovesOnlyStale` |
| watch 表在 churn 下无界增长 | 删除重建循环让 `dirs` 只增不减（issue #686） | 目录删除时 `forgetBelow` 清整棵子树；`IN_IGNORED` 清 `wdToDir` | `TestDeleteForgetsChildren` |
| watch 配额打满 | 全部同步停摆（issue #65） | `ErrWatchLimit` 降级为非致命 + `Unwatched()` + 自动每小时 `full_resync` | `TestWalkAddDegradesOnWatchLimit` |
| 事件流悄悄死掉 | 进程健康但再也不镜像 | `read()` 对瞬时错误重试 3 次，最终放弃前发 `Overflow` 并报错 | 代码约束 |
| 源根目录消失被当成「空目录」 | 目标端静默过期（或反过来被删光） | `walkCopy` 先 `os.Stat(Source)`，失败即 `refusing to sync`，目标端不动 | 真机 `rm -rf src` 实测 + `TestStartupRefusesMissingSourceRoot` |
| 根目录被删重建后永久失明 | 进程健康、计数不动、新文件永不镜像 | 四层：后端 `RootGone` → `Tree.lost` → `superviseRoots` 每 10s 巡检 → **`Tree.Verify` 比对 inode**；任一层报警即 `Reattach` + 强制全量 | 真机 `rm -rf src` 后重建实测（内核通知全丢、inode 校验兜住）+ `TestVerifyDetectsRecreatedRoot` |
| 铺镜像时挤占共置业务（java/mysql/redis） | 业务延迟被放大数倍，磁盘队列被占满 | `[global] nice` / `io_prio` → `guard.Apply` **逐线程**降优先级 | `TestApplyNiceCoversEveryThread` 从 `/proc/self/task/*/stat` 读回每个线程的 nice |
| 目标盘写满，初始同步只做了一半 | 前一半对、后一半永久缺失，扩盘也不会自愈 | `engine.Startup` 失败分支 `fails++` + `Backoff` 设 `retryAt` + `MarkOverflow()`，外部原因消失后自动全量收敛 | 真机 tmpfs 32 MiB 注入 + `TestFailedStartupArmsFullResync` |
| 文件长期被打开写（日志/WAL/大文件灌数据） | 写入期间镜像冻结在第一次快照（issue #77 / #458） | mask 加 `IN_MODIFY`（聚合折叠）+ `max_hold` 强制追平 + `copyFile` 有界拷贝 `copyBounded`（按 `stat` 到的长度拷，不追写者） | 探针实测「8 秒零事件」+ 真机 30 秒持续写入期间刷新 6 次、关闭后 sha256 一致 + 5 条用例 |
| 内存被 cgroup 限死 | OOM 被杀，或降级成不可用 | 窗口/通道/子进程输出/map/日志五处都有上限，RSS 与目录数线性、不随事件数增长 | 48 MiB 硬上限实测：峰值 24.1 MB，`failcnt=0`、`oom_kill=0`，仅变慢 |
| 系统时钟被大幅调整 | `delay` 永不结束或瞬间结束 | 全部超时基于 `time.Time` 单调读数，墙上时钟只用于状态展示 | 真机 `date -s ±2h / -30d` 后写五次，五次全部跟上 |
| `[global]` 键写错位置 | 配置看着对，进程却无 log_file / status_file | `Parse` 拒绝未知顶层键，并指出「这个键属于 `[global]`」 | `TestTopLevelGlobalKeysAreRejected` |

表中没有「内存溢出」这一行，因为它是上面几条的**结果**而不是独立原因：窗口有上限、通道有上限、子进程输出有上限、map 有清理、日志有轮转。规模实测里 RSS 只跟**目录数**走：51 万目录时整进程 142–193MB（约 280–380B/目录，§6.1），20 万文件突发溢出场景只有 83MB（目录数仅 3，§3.4），两者都不是被事件数顶上去的。

### 3.6 与共置服务共存：把干扰降下来（`internal/guard`）

前面的优化都在问「怎么更快」。这一节问的是另一个问题：**gsyncd 跑起来之后，同机的 java / mysql / redis 还能不能好好干活。**

铺一次 10T 镜像会连续几小时吃满所有核与磁盘。在一台只做镜像的机器上这没问题；在一台同时还对外提供业务的机器上，业务进程的延迟会被放大数倍 —— 它们抢不到 CPU 时间片，磁盘队列也被拷贝请求填满。这时候与其继续调吞吐参数，不如把 gsyncd 排到后面。

两个开关，都是 best effort:

| 键 | 作用 | 机制 |
|---|---|---|
| `nice = 10` | CPU 调度让给业务进程 | `setpriority(PRIO_PROCESS, tid, 10)` |
| `io_prio = "idle"` | 磁盘带宽优先给业务进程 | `ioprio_set(IOPRIO_WHO_PROCESS, tid, IOPRIO_CLASS_IDLE)` |

`io_prio` 还接受 `best-effort` / `realtime`，可带级别如 `best-effort:7`。零值（默认）表示**什么都不动** —— 专机不该被无故拖慢，这条性质有一个用例守着（`TestApplyIsInertWithoutPolicy`）。

两个实现细节值得记：

**1. 必须逐个线程设。** Linux 的 `setpriority` 和 `ioprio_set` 都是**每线程**属性，而 Go 运行时会在 OS 线程之间搬运 goroutine。只在调用线程上设一次，真正吃磁盘的拷贝 worker 仍然在默认优先级上 —— 也就是说这个功能会看起来正常工作、日志说 running at nice 10、实际一点用都没有。修法是扫 `/proc/self/task` 对每个 tid 各设一次；之后新建的线程继承父线程的 nice，所以不需要再扫。有 `TestApplyNiceCoversEveryThread` 从 `/proc/self/task/<tid>/stat` 把每个线程的 nice 读回来比对。

**2. `ioprio_set` 没有 syscall 包装，而且系统调用号按架构不同。** 用一张按 `runtime.GOARCH` 查的表（`amd64:251, 386:289, arm64:30, arm:314, riscv64:30, ppc64/ppc64le:273, s390x:282`），查不到就如实报错。为什么不钉死一个常量：错误的系统调用号会落到**另一个系统调用**上，比诚实的报错糟糕得多。

失败一律降级而不是拒绝启动：内核拒绝（容器里常见 `CAP_SYS_NICE` 缺失）、macOS 没有 io class、Windows 完全不支持 —— 全部只记一条 `cannot lower priority (nice 10, io idle): ...` 警告，同步照常进行。**没降成优先级的镜像仍然是能用的镜像**，为一个建议性参数拒绝服务是更糟的取舍。

systemd 托管时也可以用 `Nice=` / `IOSchedulingClass=`，与配置里的值不冲突（配置里的值在进程启动后再设一次，会覆盖 unit 的设置）。

---

## 4. 行为差异（迁移须知）

| 项 | lsyncd | gsyncd |
|---|---|---|
| 配置语言 | Lua（执行用户代码） | TOML（纯数据） |
| 默认后端 | `default.rsync`（必须装 rsync） | `direct`（进程内拷贝） |
| 扩展方式 | 写 Lua 函数（`action` / `init` / `full` / `collect` / `prepare`） | 实现 `sync.Backend` 接口（Go） |
| 溢出处理 | 重启进程 + 全量重扫 | 一次全量重传，进程不动 |
| 出现同步错误 | 视配置丢弃或重试 | 指数退避重试，失败路径回填队列 |
| `delete = "startup"` / `"running"` | 配置项被解析但两条路径行为相同 | 真正分离：`startup` 只清一次，之后不再删；`running` 首次不清，之后照删 |
| 初始同步失败 | 退出 | 记错误后继续，下一窗口重试 |
| 日志 | 每条 fopen/fclose | 单 writer + 轮转 + 节流 |
| 信号 | `SIGHUP` 整套重启 | `SIGINT` / `SIGTERM` 优雅退出；`SIGHUP` 交给进程管理器 |
| macOS | 私有 `/dev/fsevents`（需 root） | 快照轮询（不需要 root，但延迟受 `watch_interval` 影响） |
| 自定义 Lua action | 支持 | **不支持**（有意取舍：这正是系统依赖的来源） |
| 源/目标重叠 | 不检查 | 启动时拒绝：`target` 在 `source` 内会让每次拷贝都变成新的源文件（自我复制直到磁盘满），`source` 在 `target` 内则会被 `delete` 删掉 |

`mode = "rsync"` / `"rsyncssh"` 保留了与原版一致的参数集，因此 **rsync 侧的既有调优（压缩、带宽限制、`--exclude` 兼容写法）可以原样搬过来**。

---

## 5. 尚未实现（明确的后续方向）

按优先级：

1. **fanotify 后端**：`FAN_MARK_MOUNT` 一个 watch 覆盖整挂载点，绕开 §3.2 的 watch 上限。需要内核 5.9+ 与 `CAP_SYS_ADMIN`。
2. **分层按需 watch**：只 watch 活跃子树，冷目录靠周期性 `mtime` 扫描，把 watch 数压到工作集大小。
3. ~~**扫描游标持久化**~~ —— 已实现，见 §3.2「断点续扫」。仍有改进空间：目前只覆盖 `direct` 后端（`rsync`/`rsyncssh` 的跳过判断在 rsync 自己手里），且记录是按目录的 —— 真机实测每个目录一条记录占 **48.6 字节**（137,863 条记录、平均路径 16.6 字符，含 varint 长度与 4 字节 CRC 帧），5000 万目录约 **2.4GB 磁盘**（不是内存：记录是流式写出、读侧只有一个前向游标）。可以再加一层「整棵子树未变就只记根」的压缩。
4. **多实例分片**：一个大树按子树切给多个 gsyncd，靠 status 文件协调（原版 #202 的讨论结论）。
5. **上传/删除合并**：目前删除是单独一步。**有意不合并进 rsync 的 `--delete`**：增量批次只带变更路径，再叠加 `--delete` 会让 rsync 把目标端其余内容全部判为多余（10T 镜像上是灾难），所以远端删除走 `--delete-missing-args`，`--delete` 只出现在 Startup 的全树传输里。
6. ~~**写时延迟（issue #458）**~~ —— 已实现，键名 `close_write`，默认关闭。语义与取舍见 §3.4「等到关闭再同步（`close_write`）」。仍未做的：按路径而不是按窗口判定，以及「整窗都等」带来的批次节奏问题（见 §3.4 末尾）。
7. **符号链接策略**：目前一律不镜像（direct 与 rsync 后端的默认行为一致：都不跟随、不复制）。需要保留链接本身时，应作为显式的 `links = true` 选项加进来，而不是默认跟随 —— 跟随会把树外内容和环拷进目标端。
8. **Windows/macOS 原生后端**：非 Linux 侧只有轮询后端这一处降级 —— 每 tick 全树 walk，`watched_dirs` 只作展示。100T 规模需要换成 USN journal(Windows)/ FSEvents(macOS)的原生后端，当前实现只保证正确性。

---

## 6. 验证

```
$ go vet ./...                         # 干净
$ gofmt -l .                           # 空
$ go test ./... -count=1               # Linux 上 234 个用例(212 个 Test + 22 个 Benchmark,`go test -list` 逐包统计;
                                      # 另有 4 个 //go:build !linux 用例,跨平台合计 238)
$ CGO_ENABLED=1 go test -race ./internal/... -count=1   # 0 个 DATA RACE(gcc 4.8.5,真机)
$ bash scripts/e2e.sh                  # 真实二进制端到端
$ bash scripts/smoke.sh ./dist/gsyncd-linux-amd64   # 目标机冒烟
```

`scripts/e2e.sh` 覆盖的不只是「文件同步过去了」：它逐层断言排除规则（启动拷贝、运行期事件、目标端从未出现）、删除、以及**重命名一个目录**后目标端的新旧名字与整棵子树。这三处正是原版最容易静默出错的地方（§3.2 的 issue #225 / 排除规则一致性），所以做成硬断言而非日志观察。

交叉编译（CGO 关闭，静态）：linux/amd64、linux/arm64、linux/arm、darwin/amd64、darwin/arm64、windows/amd64 全部通过，体积 2.5–2.7MB。产物落在 `dist/`，由 `bash scripts/dist.sh` 一次生成并附 `SHA256SUMS`（避免隐藏目录里的产物在交付时找不到）。

发布二进制本身在真机上验过三件事，不只是「编译通过」：`file` 显示 `statically linked`、`ldd` 报「不是动态可执行文件」、以 `nobody` 身份执行 `-version` 同样正常 —— 也就是说把它复制到任何同架构 Linux 上就能跑，不需要 Lua、不需要 rsync 的运行时库、不需要目标机上装 Go。竞态用 `CGO_ENABLED=1 go test -race ./internal/... -count=1` 在 8 核实机上跑过一遍（十一个包全绿，`grep -c 'DATA RACE'` 为 0）：并发点集中在 watcher 的子节点索引、聚合器的窗口、引擎的 retry gate 与 status 快照，这些地方单靠 `go test` 只能证明「没崩」，证明不了「没有同时读写同一张表」。

测试能通过不等于测试有约束力，所以关键不变量还做了一次**变异检验**：把 `aggregate.go` 里 `removeAt` 的 `a.openFiles--` 删掉，`TestOpenCountStaysConsistent` 在第 7 步就报 `openFiles=1 but 0 entries carry the flag`；把 `upsertKnown` 里「重命名带过来的 open 标志」那三行去掉，`TestRenameOntoPendingPathStaysOpen` 立刻失败。两处改回后全绿 —— 也就是说这两条用例不是摆设，它们真的守着 `close_write` 唯一的判据。这类「计数器与窗口内容不一致」的缺陷不会崩、不会报错，只会让窗口要么提前放行（拷半截）要么永远不放行（等 `max_hold`），所以必须靠不变量而不是靠读代码来保证。

`scripts/smoke.sh` 是给**目标机**用的：在真实内核/arch 上建一个临时小树，依次跑 `-version` / `-check` / `-once` / 常驻进程 + 实时改文件 / `-status`，并断言排除规则与 `delete` 生效。它验证的是「这个二进制在这台机器上真的能读能写能跟上事件」——交叉编译成功和 ELF 头正确并不能证明这一点（Windows 上开发时踩到过 /tmp 的 ACL 与 MSYS 路径两种只在目标环境才暴露的坑）。

冒烟脚本里最要紧的一段是**新建目录的竞态**：它连续建 5 个目录、每个都在 `mkdir` 之后立刻写入文件，再建一条 `later/deep/deeper/x.txt`（中间目录也是新的），然后要求这些文件全部出现在目标端。`mkdir` 与第一次写入相隔几微秒，正是基于 watch 的监视器天然会丢文件的窗口；只测一个目录等于抛硬币，五个连测才有效。这条断言对应的就是 §3.2 的 issue #284 与上面 `adoptTree` 的回填逻辑——Linux 目标机上第一版正是这里失败（`dst/later/new.txt` 缺失），而不是 Windows 上跑得过的那个版本。

### 6.1 真机压测数据

测试机：Anolis OS 7.9 / kernel 4.19.91 / 8 核 / 62G 内存 / XFS（kernel 4.19 的 XFS 无 `bigtime` 以外的特殊调优），`max_user_watches` = 1048576，`max_queued_events` = 16384。测量工具在仓库外（生成器 + 场景 harness + 逐文件 `size+mtime` 比对），因为它们是压测脚手架而不是产品的一部分。

**单次变更加载延迟**（40k 叶子目录 / 40201 目录 / 120251 文件，`delay = 2s`）：

| 场景 | 变更规模 | 生成/操作耗时 | 收敛耗时 | 差异 |
|---|---|---|---|---|
| 初始全量（direct） | 120251 文件 | — | 8.0s（冷缓存 23.0s） | 0 |
| burst（新建子树） | 20000 文件 | 4.2s | 18.0s | 0 |
| 删除 | 20 棵子树 | 0.8s | 7.3s | 0 |
| 重命名目录 | 201 个目录 | 0s | 8.2s | 0 |
| 等长内容改写 | 2000 文件 | 0.8s | 6.2s | 0 |

**50 万目录规模**（`dirs=500000 -fan=2000 -size=100`，513181 个目录 / 511200 个文件 / 2.0G）：

同一棵树在两次不同的测量里给出了 5m54s 和 21m12s 两个数，差别不在代码而在**目标盘当时的状态**：前者是存储尚未退化的时段，后者是虚拟盘 `O_DIRECT` 掉到 39MB/s 之后。两个数都留着，因为后者才是当天真实可复现的那个。

| 阶段 | 结果 |
|---|---|
| 装 watch（`watching 513181 directories`） | 10s（含 2 个 adopt 池 worker 并行注册） |
| 初始全量（存储正常时段） | 5m54s，RSS 193MB，fd 7 个 |
| 初始全量（存储退化时段） | 21m12s，`/usr/bin/time -v` 给出 `USER 121s / SYS 224s / MAXRSS 12.6MB`（这一轮是 `-once`，没有 watch 索引，所以峰值远低于上面的 193MB） |
| burst 2 万文件 | 211s 收敛（生成本身约 150s），差异 0 |
| 删除 20 棵子树（约 6800 个目录） | 39s 收敛，差异 0 |
| 结束时 RSS | 142MB（`watched_dirs=513181`，约 280B/目录；100 万目录那次是 382MB ≈ 380B/目录，两者都真 —— 整进程 RSS 里固定开销与 GC 余量按规模摊薄，目录越多每目录越便宜） |

21m12s 那次专门拆过账，结论是**瓶颈不在 gsyncd 的遍历，而在目标端的元数据写入**：`filepath.Walk` 走完整棵树只要 11.5s（88966 条目/s），Go 直测 `mkdir` 是 35019/s、`open+write+chmod` 是 15930/s，而实际全量约 400 目录/s，比 syscall 能力低两个数量级。同一时段 `cp -a` 用 95–101s，也就是说这是这台机器上该起一个元数据密集型负载的固有速度，gsyncd 的额外开销（每文件多一次 `renameat`/`fchmodat`/`utimensat` 换原子性）在这个量级里被淹没了。

`strace -f -c` 对小树（6821 目录 / 6800 文件）的对照则暴露了另一个成本：**gsyncd 有 81% 的 CPU 时间花在 `futex` 和 `nanosleep` 上**（29548 / 14926 次），那是 8 个拷贝 worker 在 100 字节文件上频繁抢锁与让出；单线程的 `cp` 几乎不做这些。这解释了「小树大并发没收益」，也是把 `workers` 的默认上限压到 16 的原因；但把它调到 1 在 51 万目录上又会慢 1.8 倍 —— 同一棵树上的最优值随文件大小和深度变化，没有全局答案。

**批量删除专项**（210201 个目录 / 200000 个文件，整棵树被 `rm -rf` 掉）：

这一项专门把**磁盘**从等式里拿掉：源与目标都建在 `tmpfs`(6G)上，先跑完初始全量、静置 10 秒取空闲基线，再并行删除全部 200 个顶层目录。这样测出来的 CPU 与内存只属于 gsyncd 自己，不掺 IO 等待。

| 指标 | 结果 |
|---|---|
| 装 watch + 初始全量（210201 目录 / 200000 文件） | 10.5s（同一进程内 `watching 210201 directories` → `initial sync done`） |
| 空闲基线 | 10 秒 3 ticks（0.3% 单核） |
| `rm -rf` 本身（200 个顶层，并行） | 3s |
| 事件洪峰 → 收敛 | 16s |
| gsyncd 在这 16s 里烧掉的 CPU | **938 ticks = 9.4 秒 CPU 时间**（约 58% 单核），折合 **47 µs/目录**（9.4 秒 ÷ 20 万目录，62 万事件） |
| 聚合窗口峰值 `peak_pending` | **5,206**（不是 21 万） |
| 收敛时的一批 | **620,400 个事件 → 1 个批次 → 200 条变更** |
| `errors / retries / overflows / full_syncs` | 0 / 0 / 0 / 0 |
| RSS 峰值 `VmHWM` | 127 MB（删除过程中不涨，收敛后 75 MB） |
| 目标端残留 | 0 个文件、1 个目录（只剩根） |

三个数字值得单独解释：

- **620,400 个事件塌缩成 200 条变更。** 事件数约等于 210,201 个目录 × 2 + 200,000 个文件 —— 每个条目一条 `IN_DELETE`，每个目录再多一条 `IN_DELETE_SELF`；变更数只有 200，因为被删的是 200 个顶层目录 —— 聚合层对每条目录删除都把**该子树里待处理的项整棵吸收**（§3.2「目录删除的二次方」），不会把 21 万条逐个搬进窗口。
- **`peak_pending` 只有 5,206。** 若聚合层按事件逐条堆，这里会是 21 万量级，内存与后续批次都会跟着膨胀。这也解释了为什么 200 万个文件的树删除时窗口没有溢出。
- **`deletes = 200` 而不是 210,201。** 目标端的删除由 direct 后端按目录 `RemoveAll`，一条变更删掉一整棵子树；计的是「被删的条目」，不是「被删的 inode」。

这次测量本身踩过两次坑，值得记下来，因为两次都会**伪造出一个很漂亮的数字**：

1. 脚本里用了裸 `wait` 等 200 个后台 `rm`。但同一脚本里还后台起了 gsyncd，而 `wait` 不带参数会等**所有**子进程 —— 包括那个本该永远运行的守护进程，于是脚本挂死在删除前一步，日志里只剩「开始删除」那一行。
2. 第一次把源树指到 1T 实测正在使用的那棵树，然后在它上面删了顶层目录。删除的确实是那份数据，但被测的 gsyncd 根本不监视那份树 —— 它的 `pending` 恒为 0，看起来「收敛得极快」。

注意 `watched_dirs` 与 RSS 的关系：**每个被 watch 的目录在用户态约占 60B（索引）/380B（整进程），在内核约 100B 不可换出**，另有约 900B 的 inode/dentry 是可回收的 —— 50 万目录 = 约 30MB 索引 + 约 50MB 不可换出内核内存。三个数都由真机实测得来（`TestMemoryPerDirectory` 与 `cmd/probemark`），不再引用上游 issue 里的传闻值。

**500 万文件规模（纯元数据，把磁盘从等式里拿掉）**：源与目标都建在 `tmpfs`（`nr_inodes=20000000`，否则 1000 万个 inode 会先撞文件系统上限而不是撞 gsyncd），树形 20×20×25 个叶子目录 × 每个 500 个 0 字节文件 = **5,000,000 文件 / 10,000 目录**，配置 `delay=3 max_hold=60 delete=true workers=16 prefetch=16 max_processes=4`：

| 指标 | 结果 |
|---|---|
| 装 watch + 初始全量 | **475s**，`files=5000000`，`errors=0`，`peak_pending=0` |
| 初始全量的 CPU | 77,327 ticks ≈ **773 秒 CPU**，除以 475 秒 wall ≈ **1.6 个核**（8 核的 20%） |
| 初始全量的内存 | **峰值 `VmHWM` 40,348kB**、常驻 17–23MB |
| 空闲基线（装好后静置） | 10s 内 1–2 ticks |
| `rm -rf` 20 个顶层（即全部 500 万文件 / 10,000 个目录） | 7s 本身 |
| 事件洪峰 → 收敛 | **30s**，`events=5,020,840` → **1 个批次 → 20 条变更** |
| gsyncd 在收敛里烧掉的 CPU | **5,273 ticks ≈ 52.7 秒 CPU**，折合约 10µs/事件 |
| 聚合窗口峰值 `peak_pending` | **7,499**（不是 250 万） |
| `errors / overflows / full_syncs` | 0 / 0 / 0 |
| 目标端残留 | 0 个文件、1 个目录 |

这一项抓到过一个只在这个规模才出现的缺陷。第一版（`prefetchWindowDefault = 4096` 个目录列表）在这棵树上跑出 **`VmHWM` 1,076,996kB**，而结尾 RSS 只有 56MB、`peak_pending` 是 0 —— 说明膨胀不在聚合层而在预读：投机读取的 4096 个目录 × 每目录 500 条 ≈ 数百万个 `DirEntry`+`FileInfo` 同时驻留。修复是给预读加**条目**预算（`prefetchEntryBudget = 262144`）并把窗口降到 256；同样的树、同样的二进制之外什么都没变，峰值降到 **40,348kB（1/26）**，而 wall 从 508s 缩到 475s、CPU 从 80,681 ticks 降到 77,327 —— 三样一起变好，不是拿速度换内存。

`peak_pending = 7,499` 与 210,201 目录那次的 5,206 是同一件事的两次量测：**窗口里存的是「还有哪些路径待处理」，不是「发生过多少事件」**。5,020,840 条事件里 500 万条是文件级 `IN_DELETE`，它们所在目录被删除时整棵子树在聚合层被吸收掉，只有 20 个顶层目录留下来成为变更。

**420GB / 210 万文件的端到端实测**（真实 XFS 盘，不是 tmpfs；`/root/T/src` = 100 个顶层 × 每层 100 个二级 × 每层 21 个叶子目录 × 每叶 10 个 200KB 文件 = **2,100,000 文件 / 220,101 目录 / 420.0GB**；`delay=5 max_hold=300 delete=true checkpoint` 开启）：

| 阶段 | 结果 |
|---|---|
| 装 watch | `watching 220101 directories`，冷缓存 **421s**（同一进程热缓存时 `cmd/probemark` 只要 4.1s） |
| 初始全量 | **5h02m41.96s**，本次 `files=1,776,617`，`errors=0` |
| 初始全量期间的内存 | RSS 全程 **88–156MB**，`VmHWM` **204,420kB**，不随已拷字节数增长 |
| 初始全量期间的 CPU | 约 **10–20% 单核**（瓶颈是盘：`dm-0` `%util=100`、`await` 163ms） |
| 存量树上改 1,000 个文件 | **10s 收敛**，`applied=1000`，烧 **150 ticks ≈ 1.5 秒 CPU** |
| `rm -rf` 20 个顶层（约 42 万文件 / 84GB） | `rm` 自己 121s；gsyncd 在 **182s** 内收敛，`events=510,040`，`deletes=1,168` |
| 该阶段的 CPU | `PHASE3_CPU_TICKS=5,185` 覆盖 182 秒 ≈ **5.2 秒 CPU**，约 2.8% 单核 |
| 该阶段的内存 | RSS **97MB → 156MB**，`peak_pending` **1,145** |
| `errors / retries / overflows / full_syncs / root_gone` | 0 / 0 / 0 / 0 / 0 |

收敛后做了抽样核对（在 210 万文件的树上跑全量 `find` 会把测量拖进 D 状态，所以抽 4 个顶层目录）：`/root/T/src` 与 `/root/T/dst` 都是 **80 个顶层目录**（删掉的 20 个在目标端确实不存在），`d020/d000` 两侧都是 21 个叶子目录，`d020/d000/d000000/f000.dat` 用 `cmp` 比对 `SAME`。

这一档最值得记的有两个数。第一是**初始全量的 RSS 与已拷字节数无关**：从第一个 100GB 到最后 420GB，`rss_kb` 一直在 88–156MB 之间波动而不单调上涨 —— 拷贝路径的常驻内存由目录数（索引）与在飞并发决定，不由数据量决定。顺带一提 `files=1,776,617` 不是漏拷：这次是**续传**，上一轮中断的运行已经写了 323,391 个文件，`323,391 + 1,776,617 = 2,100,008`，与树的 2,100,000 个文件对得上（多出的 8 个是启动时 `prune` 掉的陈旧 staging 文件，也就是 `deletes=8` 的来历），计数器不跨进程累加 —— 这也是运维读 status 时必须知道的：换了进程，`files` 从 0 重新数。第二是**批量删除时 CPU 与文件数不成正比**：删掉 42 万文件只花 5.2 秒 CPU，平均 **12µs/文件**，因为 510,040 条事件里有 42 万条是文件级 `IN_DELETE`、它们在所在目录被删除时被聚合层整棵吸收，`pending` 峰值只有 1,145。

**后端隔离基准**（绕过 watcher/aggregator，直接对 `sync.Direct.Apply` 灌 40000 条变更，其中 20000 目录 + 20000 文件；全树拷贝是对 20000 文件 / 20201 目录的整棵树做 `Startup`）。测量方式：worker 数在轮次之间**轮换**（每档 4 轮取中位数），每轮用独立目标目录并在轮后删除：

| `workers` | Apply 中位数 | changes/s | 全树拷贝中位数 |
|---|---|---|---|
| 1 | 4.89s | 8179 | 2.45s |
| 2 | 3.02s | 13256 | 1.40s |
| 4 | 2.38s | 16839 | 2.03s |
| 8（默认，`min(NumCPU,16)`） | 2.72s | 14719 | 1.92s |
| 16 | 3.09s | 12958 | 1.86s |
| 32 | 2.61s | 15332 | 1.82s |

第一版这里写的是「workers 越多越慢，默认值 8 就是最快一档」——**那个结论是错的**。它是在测量机被一个占 322% CPU 的无关 java 进程拖住、load 长期在 100 以上时测的，而且各档位是连续跑的：第一档吃冷缓存，后面每一档都继承前一档 `rm -rf dst` 在 XFS 上留下的脏页与未链接 inode，于是「后测的档位更慢」被误读成「worker 越多越慢」。改成轮换档位、每轮独立目标目录之后，同样的代码得到的是：2–4 个 worker 就接近平台期（1.3–1.7 万 changes/s），再多没有收益也没有明显惩罚；1 个 worker 只有 8,179 changes/s，比平台期慢 1.6–2.0 倍。默认值 `min(NumCPU,16)` 落在平台期内，不需要调。

这个教训比数字本身重要：**在多租户或共享存储的机器上，绝对吞吐几乎不可复现，只有同一会话内轮换条件测出的相对值可信。**下方 50 万目录的表也标了测量时的 load。

好消息是**同一结论在 51 万目录的真实树上也成立**。单次连续跑（每档一次）得到的是 `workers=1` 2m21s、`2` 1m18s、`4` 1m29s、`8` 1m27s、`16` 1m36s —— 看着像「2 最优、之后递减」。改成 2 与 8 交替三轮（每轮先 `rm -rf` 目标、`sync`、等 20s 让 load 均值沉下来再开始）之后：

| 轮次 | `workers = 2` | `workers = 8` |
|---|---|---|
| 1 | 83.9s(load 21.2) | 105.8s(load 17.0) |
| 2 | 87.8s(load 6.9) | 89.7s(load 3.5) |
| 3 | 94.4s(load 7.9) | 89.9s(load 6.1) |
| 中位数 | 87.8s | 89.9s |

两档的差异在噪声内（约 2%）。所以 `workers` 的真实形状是：**1 个明显不够（慢约 1.8 倍），2 个起就饱和**；和 20000 文件规模上的平台期一致，只是大树下 1→2 这一步的收益更大（那边 1.6–2.0 倍、这边 1.8 倍，都是同一种 futex/nanosleep 争用被消掉）。

**同一棵树，规模一放大结论就变**（51 万目录 / 51 万文件，交替测量，两种顺序各一次）：

| 顺序 | `gsyncd -once` | `cp -a` |
|---|---|---|
| 先 `cp` 后 gsyncd | 132.1s(load 57) | 101.5s(load 5.1) |
| 先 gsyncd 后 `cp` | 121.3s(load 175) | 95.2s(load 13) |

也就是说：**10 万文件这个量级 gsyncd 比 `cp -a` 快 26%–55%，到了 51 万目录/51 万文件这个量级 `cp -a` 反超 10%–25%。** 原因不神秘 —— 单线程顺序写在本机的元数据路径上更友好，而 gsyncd 的并行收益早就在 2–4 个 worker 处饱和了；此时它多出来的每文件 `renameat`/`fchmodat`/`utimensat`（为原子性）就成了净成本。

这不是要掩盖的数字：gsyncd 的价值不在「比 `cp` 快」，而在于**它是个常驻镜像服务**（`cp` 只能做一次性拷贝），且提供了 `cp` 没有的：增量、排除规则、删除镜像、断点重跑、以及 234 个测试（跨平台 238 个）钉住的行为。首次全量的绝对速度是这条链上最不重要的属性 —— 它只发生一次，而且可以避开业务高峰做。

**三个测量上的坑**（写在这里免得下次重新踩）：

1. `diff -r --brief src dst` 在合成数据上会给出**假阴性**：如果所有文件内容相同（压测生成器常这么写），内容一致而 mtime 不同的文件会被判为「无差异」，于是「还没拷完」被读成「已经收敛」。判断收敛必须逐文件比 `size + mtime`。
2. 场景 harness 自己每 500ms 全树走查两遍，在 50 万目录规模下它本身就能吃掉一个核 —— 收敛耗时要标明是「含测量开销」的上界。
3. **共享主机上 `/proc/loadavg` 是全局的，不是本容器的。** 同一台机器上别的租户能把 load 从 0.3 推到 144，而本 PID namespace 里只有 300 来个进程、`cpu.max` 是 -1、`nr_throttled` 为 0、连 `/proc/pressure/io` 都没有。于是「load 低所以机器空闲」不成立，唯一可信的做法是**成对测量**：同一棵树、同一会话内交替跑 A 和 B（奇数轮 A 先、偶数轮 B 先），只看两者的相对比值，不看绝对值。
   - 这条坑真实地改过一次结论：58k 目录的切片上，交替 4 轮里 gsyncd 每轮都快于 `cp -a`（7.09/5.94/8.70/8.22s 对 15.82/8.22/10.95/12.22s）；但换到 51 万目录的全树上、两种相反顺序各测一次，`cp -a` 反而稳定快 10%–25%（101.5s / 95.2s 对 132.1s / 121.3s）。**规模变了，结论就反转** —— 所以任何「比 X 快」的断言都必须带上规模和测量条件。
   - 另外这台机器的虚拟盘 `O_DIRECT` 写只有 39.1MB/s，而 buffered 写有 1.1GB/s。任何绕页缓存的基准（带 `oflag=direct` 的 `dd`、部分 `rsync --whole-file` 路径）在这里会被放大一个数量级，不代表真实硬件。

### 6.2 10T / 200KB 小文件 / 持续写入：实测外推

问题不是「代码能不能跑 10T」，而是**这台机器能不能在合理时间内写完 10T，以及持续写入时会不会积压**。下面的基线数字都来自本机实测；标了「外推」的行写明了换算依据，不混在实测里。

先做一次换算：10T / 平均 200KB ≈ **5000 万个文件**。每目录 10 个文件就是 500 万目录，每目录 1 个文件就是 5000 万目录 —— **瓶颈是目录数和文件数，不是字节数**。

#### 实测基线

| 项目 | 实测值 | 条件 |
|---|---|---|
| 装 watch（冷缓存，边 walk 边装） | **10,780 目录/秒** | 1,001,001 个目录用 92.9s |
| 冷读吞吐 vs 并发 | 1→**12.5**，2→17.5,4→27.5,8→38.4,16→**53.5**，32→62.2 MB/s | 4000 个 200KB 文件，`dd bs=256k`，每档前 `drop_caches` |
| watch 索引（纯索引） | **61B/目录** @100 万，**47B/目录** @2000 万，**42B/目录** @5000 万 | `TestMemoryPerDirectory`（`GSYNCD_MEMDIRS` 可调规模），真机实测 2000 万目录 heap 880MB → 1822MB、5000 万目录 2320MB → 4420MB |
| 聚合窗口内存（待处理路径） | **243B/路径** @50 万，**235B/路径** @100 万，**234B/路径** @200 万，**215B/路径** @2000 万，**199B/路径** @5000 万 | `TestMillionPathsMemory`（`GSYNCD_AGGPATHS` 可调规模）；折叠耗时 3.0–3.7 千纳秒/事件，5000 万条 3m40s |
| 内核 watch 成本 | **97~115B/目录不可换出**（110 万，两次独立差分）；**104B/目录**（505 万） | `cmd/probemark`：`SUnreclaim` +104160 / +123880 kB @110 万，+512912 kB @505 万 |
| 装 watch 速率 | **73,620~76,848 目录/秒**（110 万，热缓存）；**18,027 目录/秒**（505 万，冷缓存） | 110 万 14.3~15.0s；505 万 4m40s |
| 空闲 CPU（22 万 watch 装好后静置） | **13 tick / 100s = 0.13% 单核**，RSS 120,256kB 不动 | `probemark -root /root/T/src -hold 150s`，装 220101 个 watch 用 4.08s 后测 100s |
| 整进程 RSS | **约 380B/目录**（含 Go 运行时、channel 缓冲、聚合表） + 约 100B/目录（内核，不可换出） | RSS 382MB @ 100 万目录 |
| 首次全量（200KB 文件） | **3,254 文件/秒 = 651 MB/秒** | 20000 文件 / 4.0GB，目标干净 |
| 首次全量（2KB 文件） | **1,219 文件/秒** | 100 万文件，每目录 1 个 |
| 重启重扫（目标已一致） | **2,336 文件/秒** | 只比 size+mtime，100 万文件用 7m8s |
| 持续写入 505 文件/秒 | CPU **8.5% 单核**，一条不丢 | 120s，收敛后源/目标各 106 万 |
| 持续写入 1,861 文件/秒 | CPU 35%，apply 掉到 153–429 文件/秒 | 80s，写完后约 3 分钟才收敛 |
| 裸盘持续写（`dd conv=fdatasync`） | **132 MB/秒** | 8.6GB |
| 写放大 | **2.07 倍** | 写 12GB，磁盘实写 24.8GB |
| `cp -a` 对照（200KB × 4 万） | **138 文件/秒 = 26 MB/秒** | 同一台机器、同一棵树 |

#### 结论一：能不能装下 watch —— 取决于目录数，不取决于 10T

| 形状 | 目录数 | 用户态（索引 / 整进程 RSS） | 内核内存（不可换出） | 装 watch 耗时 |
|---|---|---|---|---|
| 每目录 100 文件 | 50 万 | 30MB / 190MB | 约 50MB | 7s（7.4 万/秒） |
| 每目录 10 文件 | 500 万 | 305MB / 1.9GB | **约 520MB**（实测 104B/目录） | **4m40s**（实测 1.8 万/秒） |
| 每目录 1 文件 | 5000 万 | **2.1GB**（实测 42B/目录）/ 19GB | 约 5GB | 约 46 分钟 |

第三列的两个数不是矛盾：索引本身是 42~61B/目录，而整进程 RSS 还包含 Go 运行时按目录数增长的辅助结构（事件 channel 缓冲、聚合表、GC 余量）。真正会先撞墙的仍是第四列的内核 watch 表。

第二列不是外推：真机上把 `GSYNCD_MEMDIRS=20000000` 与 `GSYNCD_MEMDIRS=50000000` 各跑了一遍。2000 万个目录的索引 heap 从 880MB 涨到 1822MB（**47 字节/目录**），5000 万个目录从 2320MB 涨到 4420MB（**42 字节/目录**），两次都 PASS。字节数随规模**下降**而不是上升 —— 路径前缀在更深的树里共享得更多，`kids` 切片按扇出分摊，Go 的 map 负载因子在高负载时也不变。所以这一列不存在「越大越贵」的拐点，100T 的 5000 万目录按 42B 算是 **2.1GB**，而不是按 61B 外推的 3.0GB。

聚合窗口那一列量了五点：50 万条 **243B/路径**、100 万条 **235B/路径**、200 万条 **234B/路径**、2000 万条 **215B/路径**、5000 万条 **199B/路径**（`GSYNCD_AGGPATHS`；折叠耗时 3.0–3.7 千纳秒/事件，5000 万条 3 分 40 秒）。它也在随规模下降，因为路径前缀共享与 `dirNode` 复用随树深增加。所以 `max_pending = 500000` 的真实上限是 **约 120MB**（50 万条实测 243B/路径），把 `max_pending` 调到 200 万是 **约 470MB**（200 万条实测 234B/路径）。

第四列也不是外推。`cmd/probemark` 在三个规模上量了 `SUnreclaim` 的差分：110 万目录 **115B/watch**、同一棵树复测 **97B/watch**、505 万目录 **104B/watch**。三个点落在 97–115 之间，没有随规模上升 —— 这说明每个 watch 的成本是一个常数，不是「越大越贵」的曲线。真正的成本拐点在**装 watch 的速率**上：110 万目录（热缓存）7.4 万/秒，505 万目录（冷缓存、dentry 已经被自己挤出去了）掉到 1.8 万/秒，4m40s 才装完。外推到 5000 万目录，按 1.8 万/秒算是 46 分钟 —— 比早先按 1 万/秒估的 77 分钟要快，但同一个量级。所以 100T 若真有 5000 万目录，索引约 **2.1GB**（实测 42B/目录），内核不可换出的部分约 **5GB**，两者在一台 62G 内存的机器上都放得下 —— 真正的约束来自另一处：每目录一个 watch 意味着 `max_user_watches` 要给到 5000 万，而且装 watch 要跑十几分钟。

第三行的约束不是内存而是 **watch 数量**：5000 万目录要给 `max_user_watches` 留到 5000 万，内核侧约 5GB 不可换出 + 约 45GB 可回收的 inode/dentry（后者在内存压力下会被回收，不会把机器压垮），装 watch 本身按冷缓存 1.8 万/秒算是 46 分钟（热缓存 7.4 万/秒则 11 分钟）。所以在 62G 内存的机器上，**10T 数据应保证平均每目录 10 个文件以上**；如果数据形状就是「每个目录一两个文件」，更好的是换 `fanotify`（整挂载点一个 watch）或按子树分片多实例 —— 这正是把 `watcher.Backend` 做成接口的原因，但那个后端**目前没实现**（见 §5）。

watch 阶段本身是并行的：热缓存 7.4 万/秒、冷缓存 1.8 万/秒（505 万目录实测 4m40s），不需要人工分批。

#### 结论二：首次全量要多久 —— 由磁盘带宽决定，不是代码

先看读侧能压出多少。把 1T 拷贝 `kill -STOP` 住（让盘空出来），对 4000 个 200KB 文件做冷读，每档前 `echo 3 > /proc/sys/vm/drop_caches`，只改并发数：

| readers | 1 | 2 | 4 | 8 | 16 | 32 |
|---|---|---|---|---|---|---|
| 文件/秒 | 61 | 85 | 134 | 188 | 261 | 304 |
| MB/秒 | 12.5 | 17.5 | 27.5 | 38.4 | 53.5 | 62.2 |

**冷读几乎线性扩展到 32 并发** —— 单个 reader 只有 12.5 MB/s，因为每个文件都要先做一次元数据查找再做一次 256KB 读，串行时整条流水线在等 IO。16 并发拿到 53.5 MB/s，是单线程的 4.3 倍。这正是 `prewalk.go` 里「210k 目录串行 walk 139.7s → 16 worker 15.9s」的同一个现象在数据面上的版本。

但 1T 实测只有约 20 MB/s 的总吞吐（读+写合计约 40 MB/s）。把两个数放在一起就清楚了：源和目标是同一块设备（`/root/T/src` 与 `/root/T/dst` 都在 `/dev/mapper/ao-root` 上），所以 `dd` 测到的 132 MB/s 持续写（8.6GB 那一轮；存储退化前一次更短的 1GB 轮测到 95 MB/s）、62 MB/s 冷读都不能单独兑现 —— 读和写在同一个队列里抢。**20 MB/s 的读 + 20 MB/s 的写 = 40 MB/s 合计，与上面 8~16 并发的读上限吻合**，说明默认的 `workers`/`prefetch`（都是 `min(NumCPU,16)`）在这台 8 核机器上已经把盘喂饱了，再调高不会更快。

这反过来给了 10T 的一个真实判据：**如果源和目标不在同一块设备上，初始全量按读侧上限（这里 53.5 MB/s @16 并发，或更快的盘更高）算；如果在同一块设备上，按写侧上限算。** 这台机器属于后者，所以下面用 132 MB/s 的持续写与 2.07 倍写放大来估。

- 按 200KB 纯拷贝的实测 651 MB/秒：10T / 651MB/s ≈ **4.3 小时**（5000 万文件 ÷ 3254 文件/秒 = 15,366s，两条算法自洽）。
- 但这台 VM 的**持续**写只有 132 MB/秒，且写放大 2.07 倍 → 10T 真实需要 20.7T 物理写入 ≈ **46 小时**。4.3 小时那个数只在页缓存还没被写穿的前几 GB 成立。

**关键判断：gsyncd 在这台机器上已经贴着磁盘上限跑。** 证据是 `cp -a` —— 内核自带的拷贝工具，没有聚合、没有 watcher、没有 rename 原子性 —— 在同样 4 万个 200KB 文件上只有 **138 文件/秒**，而 gsyncd 是 **3,254 文件/秒**，快 23 倍。瓶颈在存储，不在实现。

要更快只有两条路，都不在代码里：（a） 目标端换更快的存储，132 MB/秒 是这台虚拟盘的上限；（b） 降低 2.07 倍写放大（XFS 日志 + 200KB 与块大小对齐），属于文件系统调优。

#### 结论三：持续写入能跟上多少 —— 约 670 文件/秒（200KB）

持续写入的上限等于「磁盘带宽 ÷ 文件大小」：132 MB/秒 ÷ 200KB ≈ **670 文件/秒**。实测 505 文件/秒时 CPU 只用 8.5%，一条不丢；1,861 文件/秒（约 370 MB/秒）时**必然积压** —— 这不是 bug，是物理上写不完，积压在聚合窗口里排队等磁盘，写完后自动收敛（实测 3 分钟内追平）。

所以：**只要上游写入速率低于「目标盘带宽 ÷ 平均文件大小」，gsyncd 就能无限期跟上，CPU 和内存都不是瓶颈。** 超过这条线，积压会一路涨到 `max_pending`（50 万），然后进入「窗口满 → 立即冲刷 → 窗口又满」的循环 —— 仍然不丢事件，但延迟持续变大。这条线必须在上线前算清楚。

#### 结论四：重启的代价 —— 实测 420GB 树 14 秒

不配 `checkpoint` 时，实测重扫已同步的 100 万文件是 2,336 文件/秒（只比 size+mtime），外推 5000 万文件就是 **21,400 秒 ≈ 5.9 小时纯扫描**，还没传一个字节。

配上 `checkpoint` 之后的实测（同一棵 420GB / 210 万文件 / 220,101 目录的真机树，先跑完上一节的 5h02m 全量，把进程杀掉再原样重启）：

| 阶段 | 结果 |
|---|---|
| 装 watch | 3s(`watching 176081 directories`) |
| 扫描与比对 | **14.4s** —— `scan state: 176000 directories verified, 176000 reused, 176081 recorded, 0 dropped` |
| 到 `phase=running` | **20.1s**（含进程启动与首轮状态写入） |
| 这一轮实际传输量 | `files=0`、`deletes=0`、`batches=0`、`events=0` |

也就是说 **5 小时的全量拷贝，重启后 20 秒就回到稳态，而且一个字节都没有重传**。`176000 verified / 176000 reused` 说明查到的每一条目录记录都命中了；`recorded` 是 176081，比上一轮的 220,101 少 **44,020** —— 上一轮删掉的 20 个顶层子树带走了这么多目录。`verified` 是 176,000 而 `recorded` 是 176,081，差的 81 条走的是正常路径（尾部没写完 / 指纹不符的那几条重新核对并重记）—— 检查点永远只是加速器，查不到就老老实实核对，不会把「不知道」当成「没变」。

**这一条已经解决**（§3.2「断点续扫」）。开着 `checkpoint` 时，内容未变的目录完全不碰目标端 —— 不 readdir、不逐文件 stat、不 prune，只 `stat` 一次目标目录、比一次时间戳与条目摘要，不符才逐项核对。

仍然要提醒运维的两点：

- 检查点只覆盖 `direct` 后端；`rsync` / `rsyncssh` 的跳过判断在 rsync 自己手里（默认 size+mtime，对 5000 万文件仍是一次全树 stat，只是不用传数据）。
- 检查点是加速器不是证据：源端仍然要整棵遍历一次（停机期间的改动只有 `stat` 看得见）。所以重启代价从「6 小时且要读两侧」降到「一次源端遍历」。

#### 结论五：1000T（约 50 亿文件）时到底哪里先崩

1000T 不是 10T 的十倍工作量，它是**另一台机器的问题**。按同样的换算（10T ≈ 5000 万文件、每目录 10 个文件），1000T ≈ **50 亿文件 / 5 亿目录**，而这个数量级下四个约束会依次撞墙，顺序和 10T 完全不同：

| 约束 | 1000T 的算术 | 先撞墙吗 |
|---|---|---|
| `max_user_watches` | 需要 5 亿；内核侧 5 亿 × 100B ≈ **50GB 不可换出**，另有约 450GB 可回收 inode/dentry | **第一个撞**：62G 的机器在装到一半时就开始回收 dentry，装 watch 变慢 |
| 装 watch 耗时 | 5 亿 ÷ 1.8 万/秒 ≈ **7.7 小时**（冷缓存实测速率） | 第二个：这期间基本没有增量能力 |
| 用户态索引 | 5 亿 × 42B ≈ **21GB** 索引；整进程 RSS 按 380B/目录算是 **190GB** | 第三个：索引本身放得下，整进程不行 |
| 首次全量 | 651MB/秒 → **17.8 天**；按这台机器真实的 132MB/秒 + 2.07 写放大 → **181 天** | 第四个，也是真正决定可行性的那一个 |

所以对 1000T 的诚实回答是：**单实例做不到，不是因为 CPU 或算法，而是因为内核 watch 表和磁盘带宽两件事都超过了单机**。gsyncd 自身在 1000T 下的行为是好的 —— 它没有 O(n²)（目录删除、rename 簿记、filter 匹配、检查点游标四处都已经改成随规模线性，见 §6.2、§3.2），内存按 42~61B/目录线性（三点实测，随规模下降），持续写入按磁盘带宽封顶；但它不能替你把 5 亿个 watch 塞进一个内核，也不能替你造一块 651MB/秒的盘。

要到 1000T，只有分片这一条路，而且 gsyncd 的结构本来就支持它 —— 每个 `[[sync]]` 是独立的 root、独立的聚合窗口、独立的检查点文件：

```toml
# 实例 1..N,每个只管一棵子树,N 取到「单实例目录数 ≤ 500 万」为止(500 万 × 100B ≈ 520MB 内核,装 watch 4m40s)
[[sync]]
name = "shard-07"
source = "/data/project07"
target = "/backup/project07"
checkpoint = "/var/lib/gsyncd/shard-07.scan"
```

分片的判据不是字节数，而是**目录数**：`watched_dirs` 单实例不要超过 500 万（内核约 520MB 不可换出、装 watch 4m40s、整进程 RSS 1.9GB），这是本仓库实测过的最大一档。超过就再切一刀。真要在单机上突破这个上限，需要的是 `fanotify` 后端（整个挂载点一个 watch，内核侧与目录数无关）或按子树动态挂载/卸载 watch，两者**都还没实现**（见 §5）。

另一条必须提醒的：1000T 下**断点续扫不是优化项而是生存项**。50 亿文件不配 `checkpoint` 的重启是 50 亿 ÷ 2,336 文件/秒 ≈ **24.8 天**纯扫描；配了之后代价降到「一次源端遍历」，而遍历本身在大树上也是按目录并行的（`prewalk.go` 的 16 路预取把 21 万目录从 126s 压到 15.9s）。

#### 汇总：能不能扛住

| 维度 | 判断 | 依据 |
|---|---|---|
| 内存 | **能**，前提是每目录 ≥10 文件 | 500 万目录 = 约 210–305MB 用户态索引（42–61B/目录，实测）+ 约 500MB 内核不可换出（另有约 4.5GB 可回收的 inode/dentry）；500 万文件端到端实测 `VmHWM` **40MB**（§6.1） |
| 事件不丢 | **能** | 溢出走全量重同步；1,861 文件/秒超载零丢失 |
| CPU | **能** | 505 文件/秒只用 8.5% 单核；超载时也只 35% |
| 批量删除 | **能**（本轮修复后） | 50 万窗口折叠 55.1ms（真机 292.6ms），原为超时 >120s；watch 表侧 100 万目录单次删除 44.1ms → 229ns；两个端到端专项：tmpfs 21 万目录全删 → 62 万事件 / 1 批次 / 200 条变更 / 16s 收敛 / 9.4s CPU / RSS 峰值 127MB；tmpfs **500 万文件**全删 → **502 万事件 / 1 批次 / 20 条变更 / 30s 收敛 / 5,273 ticks / RSS 峰值 40MB** / `peak_pending` 7,499；两者 `overflow` 与 `full_syncs` 均为 0（§6.1） |
| 源根目录被删/重建 | **能** | `Tree.Verify` 比对 inode，10s 巡检发现、20s 内重挂 + 全量收敛；实测内核通知全丢时依然生效（§3.4） |
| 首次全量 | **取决于盘** | 4.3h（651MB/s）到 46h（132MB/s + 写放大） |
| 持续写入 | **取决于盘** | 上限 ≈ 670 文件/秒（200KB）；超过就积压但不丢 |
| 重启恢复 | **能**（配 `checkpoint` 后） | 不配：5000 万文件要 5.9 小时重扫两侧；配了：实测 420GB / 220,101 目录重启 **14.4s 扫完、20.1s 回到 running、0 字节重传**（§6.2 结论四） |
| 1000T（约 50 亿文件 / 5 亿目录） | **单实例不行，必须分片** | 5 亿 watch ≈ 50GB 不可换出、装 watch 约 7.7 小时、首次全量 17.8 天（651MB/s）到 181 天（132MB/s + 写放大）；按目录数切到每实例 ≤ 500 万，见「结论五」 |
| 每目录 1–2 文件 | **勉强**（不建议） | 5000 万 watch ≈ 5GB 不可换出 + 约 45GB 可回收 inode/dentry，装 watch 约 46 分钟，`max_user_watches` 要给到 5000 万；这种形状更适合 fanotify（未实现） |

一句话：**gsyncd 本身不是瓶颈，这台机器的磁盘和「每目录文件数」才是。** 10T 里如果有 5000 万个文件、每目录 10 个以上，它能扛；如果数据形状是海量单文件目录，先解决 watch 上限，否则连启动都过不去。100T 是同一套算术放大十倍仍然可行（5000 万目录 = 2.1GB 索引 + 5GB 内核 + 46 分钟装 watch），**1000T 则必须分片**：5 亿个 watch 与 17.8~181 天的首传都不是单机问题，按目录数切成每实例 ≤ 500 万是唯一现实路径。
