# lsyncd 全仓库逐行精读与重构方案

> 仓库： https://github.com/lsyncd/lsyncd
> 检出： `lsyncd/`，分支 `refactor/modernize`，基线提交 `724f077`("apdotion message added")，版本 `2.3.1`
> 规模： 79 个文件 / 18090 行；核心代码 13275 行（C 4340 + Lua 8786 + 构建 149）
> 本文档记录：①架构与数据流；②逐文件逐行阅读结论；③缺陷清单（可复现）；④重构与优化方案。

---

## 1. 项目定位

lsyncd 是一个"事件驱动的实时镜像守护进程"。它监视本地目录树的变更事件，把事件在内存里做延迟合并（delay + combine），然后调用外部工具（rsync / rsync over ssh / cp / mv / 自定义脚本）把变化同步到目标。它**不是**双向同步工具，也**不自己实现传输协议**——传输永远交给 rsync 之类的成熟工具。

README 里有一句关键状态声明："Needs new maintainer. Up for adoption" —— 项目事实上已停止维护，这正是重构的动机之一。

## 2. 三层架构

    ┌──────────────────────────────────────────────────────────┐
    │ 层4  default*.lua   用户配置层（default.rsync/rsyncssh/direct） │
    │                      提供 action/init/full/collect/prepare    │
    ├──────────────────────────────────────────────────────────┤
    │ 层3  lsyncd.lua     引擎层（6572 行）                          │
    │      Sync / Delay / Combiner / Inlet / Tunnel / Syncs /      │
    │      Inotify / Fsevents / StatusFile / UserAlarms / runner   │
    ├──────────────────────────────────────────────────────────┤
    │ 层2  lsyncd.c       C 核心（3145 行）                          │
    │      observances 事件循环 / fork+exec / 信号 / 日志 /         │
    │      jiffies / 内嵌 Lua 字节码（runner.c + defaults.c）         │
    └──────────────────────────────────────────────────────────┘

设计哲学（docs/appendices/devdocs/index.md 原文）：**C 只做 inotify、进程管理、信号、管道、日志、闹钟这几件"必须贴近系统"的事，其余全部逻辑放在 Lua 里**。这是 lsyncd 最重要的架构约束，重构必须尊重它——不能把 Sync/Combiner 搬进 C。

### 2.1 启动时序（main → main1）

    main()                                   lsyncd.c:3060
      clocks_per_sec = sysconf(_SC_CLK_TCK)
      setlinebuf(stdout/stderr)              # 2.2.3 起保证日志行缓冲
      while (!term) main1(argc， argv)        # SIGHUP 触发整套重启
      close/remove pidfile
      return 128 + sigcode

    main1()                                  lsyncd.c:2790
      1. luaL_newstate + luaL_openlibs
      2. 版本检查 lua_version(NULL) >= 5.1
      3. 早期解析 -log / --log（必须在打开日志文件前生效）
      4. register_lsyncd(L)         → 建全局 lsyncd 表 + inotify 子表
      5. 载入 runner：--runner <file> 或内嵌 runner_out（luac 编译产物）
      6. pcall，结果存 registry[runner]
      7. 校验 lsyncd_version == PACKAGE_VERSION（防"库文件与二进制不匹配"）
      8. 载入内嵌 defaults_c（四个 default*.lua 的预编译字节码）
      9. -help/--help 由 runner.help 处理
     10. runner.configure（argv表， monitors表） → 返回配置文件名 或 直接退出
     11. realpath() 规范化配置文件路径
     12. luaL_loadfile + pcall  ← 执行用户配置文件
     13. open_inotify() / open_fsevents()
     14. 安装信号：SIGCHLD/SIGHUP/SIGTERM/SIGINT
         （SIGCHLD 特别：先 signal() 再 sigprocmask SIG_BLOCK）
     15. runner.initialize(first_time)
     16. masterloop()
     17. 清理 → lua_close

关键点：**配置文件是由 C 核心直接 dofile 的，不是 runner 干的**；runner 只负责解析命令行、准备 clSettings，并在 initialize 阶段把这些设置应用到 core。

### 2.2 主循环（masterloop， lsyncd.c:2372-2649）

每一轮严格按顺序：

    ① alarm = runner.getAlarm()            # true 表示"立刻有活"
    ② alarm 为 true → 跳过 select；否则：
       pselect(observances[last].fd + 1, &rfds, &wfds, NULL, timeout, &empty_sigset)
    ③ 遍历 observances，fd 置位则调 ready(L， obs) / writey(L， obs)
    ④ 处理延迟删除队列（nonobservances）
    ⑤ while (waitpid(0, &status, WNOHANG) > 0)
           runner.collectProcess(pid, WEXITSTATUS(status))
    ⑥ hup  → runner.hup()       （置 fade，退出后整套重启）
    ⑦ term == 1 → runner.term(sigcode); term = 2
    ⑧ runner.cycle(now) 返回 false → 退出
    ⑨ 断言 Lua 栈干净（lua_gettop == 0）

### 2.3 事件数据流（一次文件修改 → 一次 rsync）

    inotify 内核事件
      └─ inotify_ready (inotify.c)           读缓冲区，解析 struct inotify_event
          └─ handle_event： IN_MOVED_FROM/TO 配对 → "Move"，否则映射成
             Create/Delete/Modify/Attrib，调 runner.inotifyEvent(...)
              └─ Inotify.event (lsyncd.lua:4900)  wd → 绝对路径；
                 对每个 sync root 做 splitPath 得到相对路径；
                 目录自动补尾斜杠；跨 root 的 Move 拆成 Delete+Create
                  └─ Sync：delay （lsyncd.lua:2759）  过滤（excludes/filters）
                     → 从队尾向前与已有 Delay 做 Combiner.combine 合并
                     → 生成/复用 Delay，压入 self.delays(Queue)
                        └─ runner.cycle → Sync:invokeActions
                           到期的 Delay 调 config.action(inlet)
                              └─ default.rsync.action
                                 → inlet.getEvents() 取事件列表
                                 → 构造 rsync filter-list
                                 → spawn() → lsyncd.exec → fork+execvp
                                    └─ waitpid → runner.collectProcess
                                       → Sync:collect → config.collect
                                          → 'ok' / 'again'（重试） / 'die'（终止）

理解这条链是重构的前提：**性能瓶颈不在 C 核心，而在 Lua 侧的 O(n) 扫描与字符串拼接**。

## 3. 逐文件精读结论

### 3.1 lsyncd.c（3145 行）

**日志子系统**（`logcats[26]` 按首字母分桶）

- `logcats[26]` 是链表数组，`check_logcat` 未命中返回 **99**，`add_logcat` 支持 `all`(priority=99)与 `scarce`(LOG_WARNING)；类别名必须以大写字母开头。
- **`logstring0`(lsyncd.c:355-441)是最大的性能热点**：每写一条日志都做
  `fopen(settings.log_file,"a")` → `ctime()` → `fprintf` → `fclose()`,
  外加独立的 `syslog()` 调用。高负载时（每个 inotify 事件都会 log 数条）这是数百次/秒的 open/close 系统调用。
- 未 daemonize 时还会额外 `strftime("%T")` + `localtime()` 一次。

**observances 事件源注册表**(lsyncd.c:900-1210)

    struct observance {
        int fd;
        int (*ready)(lua_State*, struct observances*);
        int (*writey)(lua_State*, struct observances*);
        void (*tidy)(struct observances*);
        void *extra;
    };

- 全局 `observances[]` 是**按 fd 升序的数组**，插入靠 `memmove` → **O(n)**；每轮 select 前还要重排。
- `observance_action` 标志：回调执行期间禁止新增 observance，否则
  `error "New observances in ready/writey handlers not yet supported"` 并 `exit(-1)`。
  → 这是一个**架构级限制**，导致任何需要在回调里创建管道/fd 的扩展都做不了。
- `nonobserve_fd` 在回调期间改为压入 `nonobservances` 延迟删除栈。

**进程创建 `l_exec`**(lsyncd.c:1218-1461)

- 展开 table 参数 → 剔 nil → 支持 `"<"` 后接 stdin 文本（table 或 string）→ `fork()` + `execvp()`。
- 没有用 `posix_spawn`，没有 `O_CLOEXEC` 原子性保证（靠 `close_exec_fd` 事后补）。
- 管道写不完时用 `pipemsg` + `observe_fd` 续写（状态机塞在 `extra` 里）。
- **`lsyncd.c:1223` 每次 spawn 都调 `l_stackdump(L)`** —— 纯调试残留，生产环境每次同步都往日志刷一坨栈信息。

**jiffies 时钟**(lsyncd.c:2050-2200)

- `Lsyncd.jiffies` 用 userdata 包 `clock_t`，值来自 `times()`。
- **不是单调时钟**：`times()` 返回的是进程 CPU 时间，实际上 lsyncd 用它当墙钟用，精度只有 `sysconf(_SC_CLK_TCK)`（通常 10ms）。
- 已确认 bug：`l_jiffies_tonumber` 写成 `a1 * clocks_per_sec`（应为**除**），而且该函数**从未注册进 metatable**，是死代码。
- `l_jiffies_concat`：`snprintf(...,"%Lf", &buf)` 把指针当 long double 传。

**其他 C 侧发现**

| 位置 | 问题 |
|---|---|
| `l_free_port` | bind(0) 取端口后立即 close → 典型 TOCTOU；错误路径用 `printf` 而非日志系统 |
| `daemonize` | 自实现（注释说 `daemon(0,0)` 会误关监视 fd）；无 umask、无双 fork；`observances_len && observances->fd < 3` 时才跳过 freopen |
| `write_pidfile` | 先 `open` 后判 `fd < 0`，失败路径不 unlink 残留 pidfile |
| `masterloop` | `pselect` 返回 -1 被静默忽略（应处理 EINTR）；nonobservances 只比较栈顶一个 fd（源码注释自认 "FIXME breaks on multiple nonobservances in one beat"） |
| `l_readdir` | 返回 `{name -> isdir}`，`d_type == DT_UNKNOWN` 时 lstat 兜底（正确） |
| `main1` | 所有错误路径 `exit(-1)`，不做 lua_close / 不删 pidfile |

### 3.2 inotify.c（570 行）

- `standard_event_mask = IN_ATTRIB|IN_CLOSE_WRITE|IN_CREATE|IN_DELETE|IN_DELETE_SELF|IN_MOVED_FROM|IN_MOVED_TO|IN_DONT_FOLLOW|IN_ONLYDIR`
- `l_addwatch(L)` 参数 `(path, inotifyMode)`，支持 `""` / `"Modify"` / `"CloseWrite"` / `"CloseWrite or Modify"`；**`"CloseWrite after Modify"` 未实现，直接 `exit(-1)`**。
- `ENOSPC` 时提示调大 `/proc/sys/fs/inotify/max_user_watches` 后退出——这个提示很有用，保留。
- MOVE 配对用**全局** `move_event_buf` + `move_event` 布尔，只支持"一个未配对 MOVED_FROM"；配对失败时把 MOVED_FROM 降级成 Delete。
- `after_buf` 用**递归调用 handle_event** 处理 → 深队列时有爆栈风险。
- `IN_Q_OVERFLOW` → `runner.overflow()` + `hup = 1`（整套重启，事件全丢，这是设计妥协）。
- `inotify_ready`：`readbuf` 初值 2048，遇 `EINVAL` 翻倍；读到 `EAGAIN` 为止。
- `open_inotify` 用 `inotify_init()` 而非 `inotify_init1(IN_NONBLOCK|IN_CLOEXEC)`（非原子，靠事后 `close_exec_fd`/`non_block_fd`）。

### 3.3 fsevents.c（449 行）

- 直接对接 macOS 私有 `/dev/fsevents` 接口（10.5 时代），**需要 root**；文件头自述警告：单内核缓冲区，慢订阅者会导致 Spotlight 丢事件。
- `handle_event`：`FSE_EVENTS_DROPPED` → 日志 + `runner.overflow()` + `hup=1`；`FSE_RENAME` → "Move"，`FSE_CHOWN/FSE_STAT_CHANGED` → "Attrib"；macOS 10.15+ 会剥掉 `/System/Volumes/Data` 前缀（有专门处理）。
- `fsevents_ready` 手工按指针宽度对齐解析（**"deals with alignment issues on 64 bit by copying data bit by bit"**）。
- 复制粘贴残留：`fsevents.c` 里报错文本写成 `"inotify readbuf!=NULL in open_inotify()"`，且该 `logstring` 调用**缺分号**。



## 4. 缺陷清单（逐条可定位）

严重度：S1 数据丢失/错误镜像 · S2 功能性错误 · S3 崩溃/挂死 · S4 资源泄漏/性能 · S5 代码卫生。

### 4.1 Lua 层确定性 bug

| # | 位置 | 缺陷 | 影响 | 严重度 |
|---|---|---|---|---|
| L1 | `lsyncd/lsyncd.lua:606` | `Queue.remove` 里 `local last = nt.first`（应为 `nt.last`） | 中段删除后 `nt.last = first-1`，整个 delay 队列视图变空：事件丢失 + 后续 push 复用错误索引。**已用 Lua 5.4.7 复现**：push 1..6，`remove(3)` → size=5 但 `qpairs()` 返回空 | S1 |
| L2 | `tests/utils_test.lua` | testQueue() 只测首尾删除，且 `assert(q:size(), 4)` 第二参数是消息不是期望值，**断言实际无效** | bug 长期未被发现 | S5 |
| L3 | `lsyncd/lsyncd.lua:58-78` | `dump()` 的泛型 for 复用控制变量做 key | Lua 5.5 下报 `attempt to assign to const variable 'k'`（lupa 已触发）；5.4 恰好不报 | S3 |
| L4 | `lsyncd/lsyncd.lua:2922` / `:2581` | `Sync:delay` 的 `'remove'` 分支与 `Sync:rmDelay` 都走 L1 路径 | 见 L1 | S1 |
| L5 | `lsyncd/lsyncd.lua:~2400` | move 事件在「缓冲区读完仍无配对」时即降级为 Delete+Create | mv 一个大目录 → 目标端全量重传（#225） | S1/S4 |
| L6 | `lsyncd/lsyncd.lua:2161-2193` `toLuaPattern` + `:2289-2319` `test` | 三处叠加的匹配错误，均**已用 Lua 5.4.7 复现**：（a） `[` `]` 被转义成字面量，`tmp[0-9]` → `/tmp%[0%-9%]`，匹配 `/x/tmp5` 返回 false；（b） `test` 在模式不以 `$` 结尾时拼 `p .. '/'` 再匹配，而 `p` 自身已含尾部 `/` → `cache/` 变 `/cache//`，`/cache` 与 `/cache/x` **全都不匹配**；（c） `cache`（无尾斜杠）反而能命中 `/cache/x` | rsync 侧收到的是原始模式、行为正确，**lsyncd 自己的事件过滤却在空转**：同一份列表在「事件过滤」与「rsync 参数」两条路径上语义不一致。用户侧表现是「排除有时生效有时不生效」，且 `dir/` 这种最常用的写法完全失效 | S2 |

### 4.2 C 层缺陷

| # | 位置 | 缺陷 | 严重度 |
|---|---|---|---|
| C1 | `lsyncd.c:1223` | 每次 `l_exec` 都调 `l_stackdump(L)` —— 调试残留，生产环境每次同步刷一坨栈 | S4 |
| C2 | `lsyncd.c:355-441` `logstring0` | 每条日志 `fopen/ctime/fprintf/fclose` + 一次 `syslog` | S4（直接导致 #93 的 438GB 日志） |
| C3 | `lsyncd.c` jiffies | `l_jiffies_tonumber` 写成乘 clocks_per_sec（应除），且该函数从未注册进 metatable（死代码）；`l_jiffies_concat` 把 double 指针当 `%Lf` 传 | S2 |
| C4 | `lsyncd.c` `l_free_port` | `bind(0)` 取端口后立即 `close` → TOCTOU；错误路径走 `printf` 绕过日志系统 | S2 |
| C5 | `lsyncd.c` `masterloop` | `pselect` 返回 -1 被静默吞掉（EINTR 未处理）；`nonobservances` 只比较栈顶（源码自认 FIXME breaks on multiple nonobservances in one beat） | S3 |
| C6 | `lsyncd.c` `observances` | 按 fd 升序数组，插入用 `memmove` 是 O(n)；回调内禁止新增 observance 否则 `exit(-1)` | S3（架构级限制） |
| C7 | `inotify.c` | 全局单个 `move_event_buf`，只支持一个未配对 MOVED_FROM；`after_buf` 递归处理事件（深队列爆栈） | S1/S3 |
| C8 | `inotify.c` | `IN_Q_OVERFLOW` → `runner.overflow()` + `hup=1` 整套重启，事件全丢 | S1 |
| C9 | `fsevents.c` | 报错文案复制粘贴残留（写成 inotify readbuf!=NULL in open_inotify()），且 `logstring` 调用**缺分号** | S5 |
| C10 | `lsyncd.c` `main1` | 所有错误路径 `exit(-1)`，不 `lua_close`、不删 pidfile | S4 |


### 4.3 与 GitHub issue 的对应

| issue | 症状 | 根因（本项目定位） |
|---|---|---|
| **#725** Queue is broken， delay not at dpos 2.3.1 | 2.3.1 起 delay 报错 | 即 L1（`Queue.remove` 的 `nt.first`）。git blame 指向 commit `cda98d6b`（2022-11-09，同时引入 `Queue.inject` 与新测试，但测试没覆盖中段） |
| **#225** move 变 Delete+Create | mv 目录 → 全量重传 | L5 + C7（无 cookie 配对） |
| **#93** 438GB 日志 + `lseek` 死循环 | 大目录树初次同步 | C2（每条日志 fopen/fclose）+「waiting for N more child processes」每 60 秒一条无节流日志 |
| **#202** 125 万目录 CPU 高 | 500GB / 1.25M watch | 见 §5.2 |
| **#250** 网络断 100% CPU | rsync 秒失败→秒重试 | 无退避 |
| **#695** 批量删除卡死 | 10 万+ delete | 见 §5.3（聚合 O(n²) + 逐个 fork） |
| **#686 / #623** 内存泄漏 | RSS 单调增长 | Lua 弱表（e2d/e2d2）+ delays 队列无上限 + 每次 `l_exec` 的栈转储 |
| **#284** 新建子目录竞态 | mkdir 到 add_watch 之间的文件漏掉 | 事件驱动加 watch，无补偿扫描（gsyncd 用 `adoptTree` 回填，见 DESIGN） |
| **#466** `IN_CREATE` 与 `CloseWrite` 冲突 | 重复事件 | `standard_event_mask` 硬编码 |
| **#65 / #458 / #490 / #75** | max_user_watches、写一半就同步、3.0 提案、初始扫描并发失控 | 见 §5 |

---

## 5. 10T–100T 规模的瓶颈分析

前提换算：100TB 数据、平均文件 100KB → **10 亿文件**；平均每目录 100 个文件 → **1000 万个目录**。

### 5.1 内核 watch 上限（硬墙）

- inotify 每个被监视目录占 1 个 watch。社区口径是「约 1KB 且不可换出」，**本仓库实测（见 `docs/DESIGN.md` §6.2，`cmd/probemark`）是约 100 字节不可换出 + 约 900 字节可回收的 inode/dentry** —— 110 万目录两次差分得到 115B 与 97B，505 万目录得到 104B。1000 万目录 ≈ **1GB 不可换出**，不是 10GB。
- `fs.inotify.max_user_watches` 默认 8192（Debian 系 524288）。#65 里 17GB 数据就已经漏目录。
- 结论：**inotify 递归监视在 100T 级别代价很高但不是内存不可行**。真正的约束是三条：`max_user_watches` 要给到千万级、装 watch 要跑几十分钟到小时级（实测 1.8~7.7 万目录/秒）、以及每目录一个 watch 意味着目录数一旦超过内存能容纳的 inode/dentry，每次冷启动都要重新走一遍。可选路线：（a） 按需分层 watch（只 watch 活跃子树，冷目录靠周期性 mtime 扫描），（b） fanotify `FAN_MARK_MOUNT` 整挂载点单 watch（内核 5.9+ 支持目录事件），（c） 多实例分片（每个实例一个子树，靠 status 文件协调）。

### 5.2 初始扫描

- 原版对每个 sync 直接 `fork` 一个 rsync 走全树。10 亿文件 + 1000 万目录 = **单进程十亿次 `lstat`**，数天起步。
- #75：多个 sync 同时启动 → 无并发限制 → 磁盘队列打满。
- 优化方向：扫描与传输解耦（扫描器多线程 `readdir`，产出清单交给传输池）、按目录批处理、跳过 `mtime+size` 未变的子树、持久化扫描游标以支持断点续扫。

### 5.3 事件聚合

- `Delay` 队列靠**线性扫描**判断「该路径是否已在队列里」。#202 里 axkibe 自认：「检查每个事件是否已在队列里是 O(n²)…可以靠查找表改成 O(n log n)，但目前没做」。
- 硬上限 `maxDelays=1000`：超过就强制冲刷。10 亿文件的一次批量 `mv` 会产生**百万级事件**，于是每 1000 个事件就 fork 一次 rsync。
- `Combiner` 也是逐对比较。
- 优化方向：`path -> index` 哈希表（本项目 `aggregate` 包已实现），上限提到 50 万并配合内存水位，超限时只做「全量重传」标记而不是丢事件。

### 5.4 每次同步 fork 一个 rsync

- 1000 万目录的变更若按 1000 事件/批，就是 1 万次 `fork+exec`，每次 rsync 自己再遍历一遍参数里的目录树。
- 优化方向：进程池 + `--files-from=-` 走管道（避开 `ARG_MAX`，原版拼 argv 会静默截断）、Direct 后端（Go 内直接 `copy`，零 fork）。

### 5.5 状态无持久化

- delays 队列、`wd -> path` 映射、exclude 编译结果全在内存；进程重启 = 从头全量。100T 的一次重启是**小时级**。
- 优化方向：周期性原子写 status JSON（已实现）；未来可上 WAL 记录「已确认同步的路径水位」。

### 5.6 单线程事件循环

- `masterloop` 是单线程 `pselect`，所有 Lua 计算（过滤、聚合、决策）都在这一条线程里。大事件洪峰时，Lua GC 与表操作直接成为吞吐上限。
- 优化方向：接收 / 折叠 / 传输三层各自独立 goroutine，用有界 channel 做背压（本项目已如此）。

### 5.7 日志

- #93：每 60 秒一条 waiting for N more child processes，每条 `fopen/fclose` + `syslog`。大文件初次同步时日志涨到 438GB，把磁盘写满后 `lseek` 到 469GB 偏移死循环。
- 优化方向：单 writer + 大小轮转 + **按 key 节流**（本项目 `logx.Throttlef`）。

### 5.8 网络故障

- #250：rsync 秒失败 → 立即重试 → 100% CPU。优化方向：指数退避 + 抖动（本项目 `sync.Backoff`，1s 起、5min 上限）。

---

### 5.9 1000T（约 50 亿文件）超出单机

10T ≈ 5000 万文件、100T ≈ 5 亿文件、1000T ≈ 50 亿文件（按平均 200KB 换算）。前三档的约束都是可算的：内存按 42~61B/目录（用户态索引，三点实测 100 万 61B / 2000 万 47B / 5000 万 42B）+ 100B/目录（内核不可换出）线性增长，CPU 在上游写入低于「磁盘带宽 ÷ 文件大小」时不随规模上升。1000T 不同 —— 它需要 **5 亿个 watch**，即约 50GB 不可换出内核内存 + 约 450GB 可回收 inode/dentry，装 watch 按实测 1.8 万/秒要 7.7 小时，而首次全量按 651MB/秒要 17.8 天、按这台虚拟盘真实的 132MB/秒 + 2.07 写放大要 181 天。**单实例做不到，不是因为 CPU 或算法，而是内核 watch 表与磁盘带宽都超过了单机。**

可走的只有分片：每个 `[[sync]]` 是独立的 root、独立的聚合窗口、独立的检查点文件，按**目录数**（而不是字节数）切，单实例 `watched_dirs` 不超过 500 万（内核约 520MB、装 watch 4m40s、整进程 RSS 1.9GB —— 都是实测过的最大一档）。要在单机上突破，需要 `fanotify` 后端（整挂载点一个 watch）或按子树动态挂载/卸载 watch，两者都还没实现。完整的四步撞墙顺序与算术见 `gsyncd/docs/DESIGN.md` §6.2「结论五」。

## 6. Go 重写方案

### 6.1 目标

1. **单静态二进制**，`CGO_ENABLED=0`，不依赖 Lua/Python/系统库；配置从 Lua 换成 TOML。
2. 保留 lsyncd 的心智模型（`sync` / `delay` / `exclude` / `delete`），但实现全部重写。
3. 针对 §5 的每一条瓶颈给出对应设计。

### 6.2 分层

```
cmd/gsyncd         CLI、信号、pidfile、-check / -status
internal/engine    编排:每 sync 一个 aggregator + delay 定时器 + 传输闸
internal/watcher   Backend 接口 + 递归层(walkAdd / cookie 配对 / 溢出)
  ├ inotify_linux.go   inotify_init1 + 变长事件解析 + MoveGrace 配对
  └ poll_other.go      快照 diff(macOS/Windows/BSD 可移植后端)
internal/aggregate 哈希折叠(替代 Delay/Combiner),每事件 O(1)
internal/sync      Backend 接口:direct / rsync / rsyncssh + Backoff
internal/filter    编译一次的模式匹配(替代 Lua 逐次匹配)
internal/config    自写 TOML 解析(零第三方依赖)
internal/status    原子写 JSON 状态
internal/logx      单 writer + 轮转 + 节流
internal/event     统一事件模型(Op 位掩码)
```

### 6.3 瓶颈 → 设计对照

| 原版瓶颈 | Go 实现 |
|---|---|
| watch 上限（§5.1） | `watcher.Backend` 接口可插拔；`walkAdd` 用 worker pool 并行注册（默认 `min(NumCPU,8)`）；**ENOSPC 降级为非致命**（`ErrWatchLimit`）→ 未注册目录计入 `Unwatched()`，main 自动启用每小时一次 `full_resync` 兜底 |
| 初始扫描无控（§5.2） | 先装 watch 再做 initial sync（关闭 #284 竞态）；扫描与传输共享信号量；`poll` 后端有 `Prime()` 保证 initial copy 期间的写不被吞 |
| 聚合 O(n²)（§5.3） | `aggregate.Aggregator`：`map[path]index` + upsert；delete→create 折叠成 write；create→delete 整体消失；目录删除 `dropSubtree` 吸收全部子路径；上限 `MaxPending=500000`，超限置 **urgent**（提前冲刷窗口，**不丢事件、不触发全量**）；只有内核丢事件（`IN_Q_OVERFLOW`）才置 overflow → 一次全量 |
| 目录删除/改名的二次复杂度（本轮实测发现） | 第一版 `dropSubtree`/`followRename` 每次全窗口扫描，于是「删 N 个目录」= N × 窗口大小：窗口 20 万文件时折叠一次 **58.4s**，50 万（默认 `max_pending`）直接超时。持锁期间 watcher 灌不进事件 → 内核队列溢出 → 全量重传，这正是 issue #695「批量删除时卡死」的成因。改为显式目录树索引（`nodes map[string]*dirNode`，节点在有条目时创建、子树空了自底向上回收）后：20 万文件 **23.8ms**、50 万 **55.1ms**，严格线性；目录改名 50 万文件 37.3ms。真机（8 核 / kernel 4.19）重测同样线性但慢三到五倍：删 10 万目录 **292.6ms**、改名 50 万文件 **271.6ms**，即每目录 2–3µs，与规模无关。插入仍是 O(1)（`BenchmarkAddLarge` 窗口 1000 → 10 万仅从 700ns 升到 1150ns/op） |
| fork 风暴（§5.4） | 默认 `direct` 后端（Go 内 `copy` + `.tmp` + `fsync` + `rename`）；rsync 走 `--files-from=-` 管道 + `BatchLimit` 分片 |
| 状态无持久化（§5.5） | `status.Writer` 周期原子替换 JSON；`-status` 子命令可读 |
| 单线程（§5.6） | watcher goroutine → 有界 channel → engine ticker，每 sync 一个 delay 定时器，传输由 `sem chan struct{}` 限流 |
| 日志（§5.7） | `logx`：单 writer、按大小轮转、`Throttlef(key, interval, ...)` |
| 网络故障（§5.8） | 引擎侧 `applyRetry` + `sync.Backoff(1s..5min)` + 抖动；失败批次 `readd` 回聚合窗口（只重试这些路径） |
| 溢出即重启（C8） | `watcher` 把 Overflow 事件透传给 engine → `MarkOverflow()` → 该 sync 下一批 `Batch{Full:true}` 全量重传，**不重启进程、不丢事件** |
| MOVE 降级（L5/C7） | `inotify` 后端 `pending map[cookie]` 精确配对；`MoveGrace=750ms` 超时才转 Delete；配对成功后 watch 表（`Rekey`）与待处理项（`followRename`）都跟着改名，跨 sync 的 move 拆成 delete+create；rename 在目标端没有旧名字时回退成整棵拷贝，该回退路径同样应用排除规则（且要把子树相对路径重定基到同步根，否则规则静默失效） |
| 部署依赖（§6.1） | 单静态二进制 `CGO_ENABLED=0`，不链接 liblua、不需要 Lua 解释器；配置 TOML 纯数据；默认后端进程内拷贝，连 rsync 都不需要 |

### 6.4 已验证

- `go vet ./...` 干净；`gofmt -l .` 为空；`go test ./...` 在真机（Linux）上 **234 个用例**全绿（212 个 Test + 22 个 Benchmark，按 `go test -list` 逐包统计；另有 4 个 `//go:build !linux` 用例，跨平台合计 238。逐包：cmd/gsyncd 8、aggregate 34+4、config 19、engine 41、filter 6+8、guard 7、logx 10+2、scanstate 7、status 4、sync 57+4、watcher 19+4）。规模用例：真机 `GSYNCD_AGGPATHS=1000000` 实测 100 万路径折叠 **235B/条常驻**（50 万条处是 243B/条，200 万条处 234B/条；耗时在 3,000–3,800ns/条之间波动，单次跑含 GC，机器间差异大）；`GSYNCD_AGGPATHS=20000000` 为 **215B/条、4173ns/条**；一次性删除 10 万文件集中到一个子树事件。
- Windows 上真实二进制端到端跑通（启动全量 → 改文件 → 新增 → 新建子目录 → 删目录 → 重命名 → `-status`），脚本见 `scripts/e2e.sh`（在 gsyncd 仓库内）。
- 交叉编译全通过（静态、无 CGO，均 2.5–2.7MB）：linux/amd64、linux/arm64、linux/arm、darwin/amd64、darwin/arm64、windows/amd64；六个目标平台 `go vet` 均无输出。
- 实测：20MiB 大文件跨 `fsync` 阈值同步后 `cmp` 一致；符号链接一律不镜像（避免把树外内容和环拷进来）；5 万文件首次全量 11.1s，对照 `cp -r` 35.7s。
- **真机压测**（Anolis 7.9 / kernel 4.19 / 8 核 / 62G / XFS）：12 万文件初始全量 8s；2 万文件 burst 18s 收敛、删 20 棵子树 7s、重命名目录 8s、2000 个等长改写 6s，差异全部为 0；**51 万文件 / 51 万目录**装 watch 10s、初始全量 1m18s–2m21s（取决于 `workers`，见下）、RSS 142–193MB；后端轮换测量则显示 2–4 个 worker 即平台期（1.3–1.7 万 changes/s），1 个 worker 慢 1.6–2.0 倍。详见 `gsyncd/docs/DESIGN.md` 的 §3.3 与 §6.1。
- 同一棵 51 万目录的树交替对照 `cp -a`：**两种相反顺序下 `cp -a` 都快 10%–25%**（101.5s/95.2s 对 132.1s/121.3s），而在 10 万文件量级上结论相反（gsyncd 快 26%–55%）。gsyncd 的定位是常驻镜像服务而不是一次性拷贝器，首次全量的绝对速度是它最不重要的属性。
- 真机上另外修掉五个只有目标内核才暴露的缺陷（事件优先于 `stat`、`AddWatch` 的 inode 复检、`IN_DELETE_SELF` 的 `IsDir` 与 `IN_IGNORED` 清表、快照后端不是逐目录 watch、rsync 后端的 rename/目录删除语义），见 DESIGN.md §3.3。
- **批量删除专项（真机，`/root/gs18`，`benchtime 1x`）**：`BenchmarkMassDeleteAbsorb` 在 1 万 / 10 万 / 20 万 / 50 万个窗口文件（对应 2 千 / 2 万 / 4 万 / 10 万个待删目录）上分别是 **4.09ms / 60.4ms / 113.8ms / 292.6ms**，每目录 2–3µs 且不随规模上升；`BenchmarkMassRenameDir` 10 万 / 50 万文件是 **54.1ms / 271.6ms**。同一场景修复前是 58.4s（20 万）与 >120s 超时（50 万），且全程持聚合锁 —— 这就是 issue #695 的成因。watcher 侧的子节点索引在同一台机器上是 100 万目录单次删除 **44.1ms → 229ns**。详见 DESIGN.md §3.2。
- rsync 后端在真机上暴露出三个语义坑：`mv` 只传新名字不删旧名字；`--delete-missing-args` 删不掉非空目录却返回 0；`--files-from` 抑制 `-a` 隐含的 `-r` 导致运行期新建目录内容为空。三者均已修复并有单测钉住。

