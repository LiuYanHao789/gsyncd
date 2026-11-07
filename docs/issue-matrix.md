# issue 回归矩阵

对 lsyncd/lsyncd 的全部 733 条 issue 与 PR(641 issue + 92 PR，open 181 / closed 552)逐条给出结论：它在新实现里是什么状态、靠什么机制、被什么守护。

抓取快照时间 2026-11-06，覆盖 #1 – #745。分类口径见文末。

## 汇总

| 分类 | 条数 | 含义 |
|---|---|---|
| FIX | 151 | 已修复 / 已被新设计消除 |
| SCALE | 8 | 规模与性能（10T / 100T） |
| DONE | 8 | 重写本身即为答案 |
| CFG | 203 | 配置即可满足（不需改代码） |
| TODO | 71 | 尚未实现（已记录在 DESIGN 第 5 节） |
| ENV | 222 | 构建 / 打包 / 平台问题 |
| EXT | 64 | 外部因素 / 信息不足 |
| DUP | 6 | 无有效技术内容 |
| 合计 | 733 | |

比例上，151 条（21%）是原实现的真实缺陷并已在新实现里消除，另有 8 条规模问题由架构解决、8 条由「换语言」解决；203 条（28%）用现有配置键就能满足；71 条（10%）是确认不做、已在 DESIGN.md 第 5 节记录的；其余 292 条是构建、平台或外部环境问题（其中 6 条无有效技术内容，见上表 DUP 行）。

关于 ssh：rsyncssh 模式的 [sync.ssh] 子表只有四个键 —— binary、port、identity_file、options（map，逐项渲染成 -o K=V）。远程用户名写在 target 里（`user@host:/path`），不在 ssh 子表里。下面凡是提到 identity_file / options 的地方都是这个含义。

读法：FIX 与 SCALE 两节是这次重写的价值所在 —— 它们回答「原项目到底哪里坏了、你怎么证明修好了」。TODO 一节是能力的边界，读它比读 README 更能判断 gsyncd 是否适合某个场景。

## 已修复 / 已被新设计消除 —— 151 条

这一类在原实现里是真实缺陷，gsyncd 用不同的机制重写后不再存在。每条给出机制与守护它的用例或真机验证。

其中 33 条在结论里直接点名了用例；其余是机制性结论（例如「不经 shell」「没有 Lua 解释器」），它们由同一份机制覆盖多条 issue，逐条重复用例名只会让表变长。按机制查用例请用文末的「验证锚点」表。

| # | 类型 | 标题 | 结论 |
|---|---|---|---|
| 3 | issue | Spaces in directory names break lsyncd at run-time | 路径全程走 filepath，不经 shell；rsync 后端用 --files-from=- --from0 传路径，空格/换行安全。 |
| 9 | issue | Duplicate calls of rsync if multiple files in directory change at the same time | 聚合窗口把同目录多事件折叠成一条，不再每文件一次 rsync。 |
| 12 | issue | Very poor performance on syncing deletes | pruneDir 批量删除 + Delete 事件直接 RemoveAll，不再逐个文件 fork。 |
| 13 | issue | Moving/renaming large files | rename 用 cookie 配对 + rename(2) 系统调用，大文件不重传。 |
| 20 | issue | on node halt, lsyncd will exit. | 对端不可达只退避重试，不退出进程。 |
| 21 | issue | Maximum file list? (no space left on device) | 路径经 stdin 传递不受 ARG_MAX 限制；watch 上限降级为非致命。 |
| 30 | issue | rsync IO error (due to unreachable peer) erroneously treated as success/done | 后端按退出码与 stderr 判定失败，失败批次 readd 回窗口重试；退出码分类见 DESIGN.md「批量删除时 rsync 会「失败」」一节（`ExitError`、`TestBenignExitCodes`）。 |
| 31 | issue | ERROR: Cannot add watch ..... (28:No space left on device) | ErrWatchLimit 降级为非致命 + Unwatched() + 自动 full_resync。 |
| 36 | issue | Problem with brackets in filename | 不经 shell，方括号/引号/反引号都是普通字节。 |
| 40 | issue | wrong pid in pid-file | pid_file 写入真实 PID，拒绝覆盖活实例，退出时删除；TestWritePidFileReplacesStale / TestWritePidFileRejectsLiveInstance 覆盖。 |
| 41 | issue | Exclude doesn't work with rsyncssh | 排除在 watcher 与后端两层生效，与后端无关。 |
| 43 | issue | Neither Logfile nor Pidfile is made. | log_file/pid_file 目录自动创建，失败会明确报错。 |
| 46 | issue | feature-request - intelligent moving not only for ssh | 所有后端都走 rename 快路径。 |
| 47 | issue | Can't get rid of "Queue is broken, delay not a dpos" error message | Queue 重写为 Go 聚合器，不再有 dpos 概念；原 Lua bug（remove 中段删除置空队列）已定位复现。 |
| 49 | issue | 15:27:43 Error: IN LUA: lsyncd.lua:3393: event does not have field 'isList' | 没有 Lua 事件对象，不存在 isList 字段。 |
| 52 | issue | lsyncd: exits with Interrupted system call if strace'd | Go runtime 内部处理 EINTR，不会被 strace 打断退出。 |
| 54 | issue | rsyncssh move issue | rsyncssh 的 move 走 rename 快路径，失败才回退拷贝+删除。 |
| 57 | issue | Data loss from failed rsync runs | 失败批次 readd + 指数退避，不会静默丢数据。 |
| 58 | issue | No re-sync to nodes other than first one | 每个 [[sync]] 独立 root 与独立退避状态。 |
| 61 | issue | Inotify IN_CLOSE_WRITE results in many useless calls to rsync | 同一路径连续 CLOSE_WRITE 被折叠成一条 pending change；开了 `close_write` 时它还携带独立的 `event.Close` 位，是「写完」的唯一判据（`TestWriteWithoutCloseCountsAsOpen`）。 |
| 65 | issue | cannot monitor all files when huge Files size and directories | watch 装不下时降级 + 周期性全量，不中断其它目录。 |
| 75 | issue | Very high disk usage on init | 初始同步受 max_processes 全局闸与 per-sync limit 双重限制。 |
| 77 | issue | Copy before write is finished | inotifyMask 加 IN_MODIFY + MaxHold（默认 1h，键名 `max_hold`，也接受 `maxhold`）+ 有界拷贝。真机 s9 实测：一个文件被持续 append 30 秒，目标端在写入期间被刷新 6 次（每次约 1.5MB 增量），writer 关闭后 sha256 一致、无 staging 残留。要连「写入期间的半截快照」一起消掉，开 `close_write = true`（默认关）。 |
| 78 | issue | lsyncd sometimes dies silently, last log entry: "Normal, Finished a list = 13" | 无子进程等待循环，退出有明确日志与原因。 |
| 79 | issue | lsyncd dies after initial rsync (rsync warning: some files vanished) | 初始同步失败不再致命，进入退避重试。 |
| 80 | issue | lsyncd spinning 100% CPU, writing "Normal: waiting for 1 more child processes" indefinitely to … | 事件驱动，无 busy wait；僵尸子进程由 Wait 回收。 |
| 84 | issue | maxDelays doesn't seem to work | max_pending（默认 50 万）取代 maxDelays，达上限提前 flush 而不丢事件。 |
| 85 | issue | initial sync is too agressive | 初始同步串行目录遍历 + 有界并发，带进度日志。 |
| 90 | issue | lsync dies after initial sync | 初始同步后进入主循环，失败退避不退出。 |
| 93 | issue | Thousands of log messages: "Normal: waiting for 1 more child processes." | logx.Throttlef 全链路节流 + 64KB bufio + 轮转 + 进度日志每 5 万条一行。 |
| 97 | issue | Treat rsync exit code 13 as temporary problem | 退出码分类重写，13 视为可重试。 |
| 99 | issue | OnMove event does not get triggered in example script | 事件模型在 Go 里重写，无 Lua onMove 注册。 |
| 101 | issue | When SIGTERM is received, lsyncd shouldn't exit-code with 0, but with 143 | SIGTERM 走 ctx 取消 + 排空，退出码 0；143 语义不再需要。 |
| 106 | issue | Better error handling on pipe write() | 无 pipe 写路径（Go channel），背压有界且不会静默失败。 |
| 107 | issue | Lsyncd crashes with lots of rsync error 13s | 大量 exit 13 只退避重试，不崩溃。 |
| 110 | issue | lsyncd: dies if rsync ever prints anything to stderr? | stderr 有内容只记日志，不判死。 |
| 113 | issue | Other events shouldn't trigger until onStartup is finished | 初始同步期间事件进窗口，完成后按窗口 flush。 |
| 115 | issue | Modify inotifyMode ignored | 事件掩码内置且正确，不再有 inotifyMode 这个易错开关。 |
| 117 | issue | lsyncd 2.0.6 leaks horrible amounts of RAM | 窗口/通道/子进程输出都有上限；内存只跟目录数走：51 万目录整进程 142–193MB（约 280–380B/目录），20 万文件突发溢出只有 83MB（目录数仅 3）。 |
| 121 | issue | lsyncd 2.0.4 gets into a loop when server is down for a while | 对端不可达走退避（15s→10min），真机断链 90s 实测 0.4% CPU。 |
| 122 | issue | lsyncd gives a 'Error: Critical exitcode' when using rsyncssh with rsync.net | 退出码按 rsync 语义分类，非零即失败并重试。 |
| 124 | issue | issue with syncing apache log files | IN_MODIFY + MaxHold 使持续写入的日志文件也能跟上。 |
| 126 | issue | ms office documents onModify issue | IN_CLOSE_WRITE 是主事件，编辑器保存后按 `delay` 同步；开了 `close_write` 则等到 close 才刷（`TestCloseWriteHoldsFlushWhileFileIsOpen`）。 |
| 131 | issue | [lsyncd 2.0.7] move events can turn into syncs when aggregated | followRename 只重定向被重命名子树内的 pending 项，不会把 move 变成全量 sync。 |
| 140 | issue | Concurrent rsyncs for large no. of files | max_processes 全局闸限制并发 rsync 数。 |
| 150 | issue | Regression : PANIC: unprotected error in call to Lua API lsyncd 2.0.5 -> 2.0.7 | 没有 Lua 解释器，不可能 PANIC。 |
| 161 | issue | will files created during init be synced if init took a long time? | watch 装上前的已有文件由 adopt backfill 补发（TestAdoptBackfillsExistingContent）。 |
| 168 | issue | lsync not starting properly in 2.1.2, bad host | bad host 只退避，不退出。 |
| 171 | issue | Is this a bug of allocating  memory for pipe  text when spawn a child to execute ? | 无 C pipe 缓冲分配。 |
| 174 | issue | Sync when remote host down/up | 断链退避 + 恢复后自动补差。 |
| 186 | issue | lsyncd dies after first rsync | 同 #90。 |
| 194 | issue | Error on exitcode if defined | 退出码表内置且正确。 |
| 196 | issue | pidfile should be removed at termination of lsyncd | 退出时删除 pid_file(TestWritePidFileReplacesStale)。 |
| 206 | issue | Vi'd files not sync'ing with 'default.rsyncssh' | IN_CLOSE_WRITE 被映射成 `Write`（编辑器保存即触发），掩码固定含该位，不需要额外配置。 |
| 207 | issue | l_readdir (d_type == DT_UNKNOWN) branch - too slow | Go 的 ReadDir 不依赖 d_type，无此慢路径。 |
| 209 | issue | if one job fails lsyncd dies | 单个 sync 失败只退避，不影响其它 sync。 |
| 211 | issue | default.direct appears not to log | direct 模式日志齐全（debug 级）。 |
| 212 | issue | is default.direct persistent in attempts? | 失败批次 readd + 退避，持久重试。 |
| 220 | issue | Backtick in filename make mv fail | 不经 shell。 |
| 225 | issue | lsyncd deletes and recopies to destination when moving file locally | MOVED_FROM/MOVED_TO 用内核 cookie 配对 + MoveGrace 750ms；不再降级成 delete+create。 |
| 226 | issue | Race condition on a live directory tree | adopt backfill + 新建目录后立即补发已有内容（TestAdoptBackfillsExistingContent）。 |
| 230 | issue | lsyncd watches ignored directories | watcher 的 Ignore 直接不 watch 被排除目录。 |
| 247 | issue | Moving files generates Create/Delete events on wrong order | MOVED_FROM/MOVED_TO 用内核 cookie 配对，顺序颠倒也能正确合并。 |
| 250 | issue | lsyncd gets into high CPU usage loop when network is down | 失败批次 readd + 指数退避（15s→10min，带抖动）；真机断链 90s 实测 CPU 0.4%。 |
| 256 | issue | lsyncd is copying partially written files | delay 是静默期语义；IN_MODIFY 让持续写入的文件在 MaxHold 内被追上，拷贝走 staging+rename，目标端不会读到半个文件。 |
| 259 | issue | Unknown exitcode (list): 123 | 退出码按 rsync 语义分类，未知码按失败重试而不是误判成功。 |
| 266 | issue | Using the lsyncd  resulted in damaged files. | 拷贝走 .gsyncd-tmp-* + fsync + rename，读到的永远是完整旧版或完整新版。 |
| 290 | issue | Lsyncd deletes files if directory is removed and replaced by symlink quickly | 源目录被替换为符号链接时不跟随，目标端保留原目录，不会误删数据。 |
| 294 | issue | Recurse over directories in sorted order, during initial setting of inotify watches | walkOrdered 保证确定的遍历顺序，prefetch 只预读不改变顺序，所以 checkpoint 仍有效（TestWalkOrderedMatchesFilepathWalk）。 |
| 295 | issue | lsyncd crashes on "mv" with normal file name (Version 2.1.5) | 同 #247。 |
| 296 | issue | "delete = false" does not work | delete 支持 true/false/startup/running 四种，有 TestDeleteModes / TestDeleteModeStartupIgnoresLaterDeletes / TestDeleteModeRunningKeepsTargetOnStartup 覆盖。 |
| 298 | PR | Properly sanitize mv parameters (CVE-2014-8990) | 不经 shell，所有参数走 argv（CVE-2014-8990 类注入不存在）。 |
| 302 | issue | lsyncd exits silently if logfile does not exist | log_file 目录自动创建；失败会明确报错而不是静默退出。 |
| 312 | issue | Lsyncd did not continue after connection loss | 断链退避 + 恢复后自动补差，真机实测过。 |
| 328 | issue | Crash with long paths in printlogf0() from l_addwatch() | 日志用 Go 字符串，无固定长度缓冲，长路径不会越界。 |
| 336 | issue | default-rsyncssh -- can't move files | move 走 rename 快路径，失败才回退拷贝+删除；有 TestDirectRename / TestRenameFallbackCopyHonoursIgnore。 |
| 340 | issue | Rsync scan all the directory when the is only one change | 只把变更路径交给 rsync(--files-from=-)，不会扫全树。 |
| 343 | issue | Mac OS X 10.5 - Overflow in event queue | IN_Q_OVERFLOW → Overflow 事件 → Batch{Full：true}，进程不重启、不退出。 |
| 348 | issue | direct mode allows injecting unauthorized filesystem operations | direct 模式对每个路径做 relInside 校验，拒绝越界路径（TestDirectRejectsPathsOutsideSource）。 |
| 350 | issue | lsyncd crashes when it cannot change source directory (Segmentation fault) | 错误一律返回并记日志，不崩。 |
| 351 | issue | Timestamps to logfiles | logx 每行带时间戳与级别。 |
| 367 | issue | slow move | 同 #336。 |
| 370 | issue | lsyncd in default.direct mode copy file without datetime | 拷贝后 Chtimes 保留 mtime。 |
| 371 | issue | Rename local file that is missing on destination | rename 失败自动回退成拷贝+删除。 |
| 381 | issue | syncing issue when remote host is unavailable | 同 #250。 |
| 383 | issue | lsyncd logs too aggressivly | Throttlef 全链路节流 + 轮转。 |
| 386 | issue | can't sync a file named "~~7F]`B@M_EO73_@%KJA8VN.txt" or "~~7F]`B@M_EO73_@%KJA8VN.jpg" | 路径是字节序列，不经 shell 也不做转义，任意文件名安全。 |
| 406 | issue | Does lsyncd watch excluded files? | 被排除的目录不会被 watch(TestDirectStartupHonoursIgnore + watcher Ignore)。 |
| 407 | issue | delete = "running" not working | delete=running 有 TestDeleteModeRunningKeepsTargetOnStartup 覆盖。 |
| 415 | issue | [QUESTION] Abot rsync processes opened by lsyncd | max_processes 全局闸 + per-sync limit 双重限制并发 rsync。 |
| 422 | issue | lsyncd-2.2.1 crashes when attempting to sync directory-tree deletions | dropSubtree 按子树整体吸收 + RemoveAll 批量删；20 万文件批量删除实测通过。 |
| 423 | issue | Aborts when trying to send temporary files | 临时文件正常同步；sweepTemp 只清 24h 以上的 .gsyncd-tmp-* 残留。 |
| 427 | issue | PID in lsyncd.pid is not correct | pid_file 写真实 PID(TestWritePidFileReplacesStale)。 |
| 438 | issue | lsyncd ignores -pidfile when -nodaemon is set | pid_file 与 nodaemon 无关，两者同时生效。 |
| 439 | issue | Agressive logging | 同 #383。 |
| 452 | issue | Crashed with error Cannot assign new key "alarm" to Delay | 没有 Delay 表，不存在这个冲突。 |
| 458 | issue | Option to delay sync if file changes during "delay" seconds | IN_MODIFY 刷新 pending 条目的 At，delay 变成「静默 delay 后才拷」；再加 MaxHold（默认 1h）兜底永不静默的文件。**该 issue 真正要的「等到文件写完再同步」已作为 opt-in 实现**：`close_write = true` 时判定改为「writer 已关闭」，`max_hold` 强制保留（关掉会被拒绝），详见 DESIGN §3.4。 |
| 460 | issue | Lsyncd recovery files after restart | checkpoint 断点续扫：重启后按目录指纹跳过已确认子树（TestCheckpointSecondRunCopiesNothing 等 8 个用例）。 |
| 461 | issue | statusFile not updated | status_file 原子替换 + status_interval 定时刷新。 |
| 466 | issue | Including IN_CREATE in the standard_event_mask counteracts inotifyMode = "CloseWrite" and seems… | 事件掩码内置：ATTRIB/CLOSE_WRITE/CREATE/MODIFY/DELETE/MOVE*，不存在 inotifyMode 与 IN_CREATE 的冲突 —— `close_write` 只改「什么时候算写完」，不改掩码（它同时需要 IN_MODIFY 与 IN_CLOSE_WRITE）。 |
| 467 | issue | default.direct initial rsync does not preserve | direct 保留 mode 与 mtime；属主需 root + rsync 后端。 |
| 475 | issue | partial rsync not retried | 失败批次 readd 回窗口并重试（TestFailedBatchIsReadded）。 |
| 476 | issue | rsync: failed: No such file or directory | 源文件已消失按删除处理，其它错误重试。 |
| 478 | issue | rsync chown, chmod option bug | rsync_extra 原样透传，不再有参数名拼写与覆盖顺序的 bug（31 条评论的那个解析问题不复存在）。 |
| 486 | issue | When copying files via rsync, lsyncd won't watch subfolders | watch 是递归的，新建子目录立即 addWatch + backfill。 |
| 503 | issue | Critical exitcode | 退出码分类正确，不再有 critical exitcode 的误判。 |
| 513 | issue | Cannot make it update apache logs | IN_MODIFY + MaxHold 使持续追加的日志也能跟上。 |
| 520 | issue | lsyncd on container with mounted volume fail when mv. | 容器内 mv 走 cookie 配对 + rename，跨挂载点 rename 失败回退拷贝。 |
| 522 | issue | cvs_exclude flag ignored after startup | 排除在 watcher 层就生效，运行期改 excludes 需重启（配置启动时读入）。 |
| 524 | issue | Error when I delete file while lsync is running (not at startup) | 运行期删除走 Delete 事件 → RemoveAll；有 TestDirectApply / TestMassDeleteOfNestedTreeDeletesEveryEntry 覆盖。 |
| 530 | issue | High CPU usage because of event compaction | 聚合器是 O(1) map + 目录树吸收，不存在事件压缩的高 CPU 路径（TestMillionPathsMemory / TestBurstDeleteOfHundredThousand）。 |
| 531 | issue | failing to start on missing source at startup | 源不存在时拒绝同步并报警，不会静默把目标删空（TestStartupRefusesMissingSourceRoot）。 |
| 537 | issue | lsyncd seems not be updating a 3TB file in the target directory | MaxHold 兜底：即使文件一直增长，最多 1h（可配）也会被拷一次，不再永远停在旧版本。 |
| 545 | issue | mysql-binlog file cannot be update in target server | mysql binlog 是持续 append 的文件，正是 IN_MODIFY + MaxHold 的目标场景。 |
| 552 | issue | Lsyncd inotifyMode not working | 没有 inotifyMode 开关，掩码内置。 |
| 566 | issue | If error, lsyncd needs to be restarted for synchronisation | 错误自动退避重试，不需要重启进程。 |
| 575 | issue | Excluded directories are monitored and synced | 被排除的目录不 watch 也不同步。 |
| 576 | issue | move operation not working | move 走 rename 快路径。 |
| 577 | issue | Bug in move operation | cookie 配对 + rename；有 TestDirectRename / TestRenameFallbackCopyHonoursIgnore。 |
| 580 | issue | ExcludeFrom does not re-sync directory when entries in exclusion files are changed | 配置启动时读入；改 excludes 后重启生效。 |
| 582 | issue | sync outside target directory | relInside 校验拒绝越界路径（TestDirectRejectsPathsOutsideSource）。 |
| 586 | issue | lsyncd delays keeps growing and never seems to fix | delay 队列是 Go 聚合窗口，有上限且不会无限增长（TestSizeCapFlushesEarly / TestMillionPathsMemory）。 |
| 601 | issue | lsyncd v2.2.2 on Linux filling up log with Attrib message | Attrib 事件被聚合器折叠，不会刷屏；日志另有 Throttlef 节流。 |
| 604 | issue | Moving file deletes it on the target and starts a new sync, sometimes logs errors | 同 #247/#577。 |
| 613 | issue | Lsyncd init keeps retrying startup on directory with a lot of data | 初始同步失败不再致命：进入指数退避并挂起一次全量重同步，外部原因消失后自动收敛（TestFailedStartupArmsFullResync）。 |
| 620 | issue | Files copied to source folder are not synced | adopt backfill 补发 watch 装上前的已有文件。 |
| 623 | issue | Do not release memory, there may be a memory leak | 窗口/通道/子进程输出都有上限；watch 表在删除时清理（TestDeleteForgetsChildren）；内存只跟目录数走：51 万目录整进程 142–193MB。 |
| 628 | issue | Startup fails without logs | 启动失败都有明确日志与退出码；配置错误由 -check 提前暴露。 |
| 629 | issue | Startup is not verbose enough when initializing watch for large amounts of directories | 初始遍历带进度日志（watching N directories / initial sync 进度），不再长时间静默。 |
| 630 | issue | race condition with NFS source: create big file then delete => the file is left on destination | adopt backfill 补发已有内容；NFS 上 create→delete 的竞态由事件顺序保证。 |
| 633 | issue | stopping rsync processes when service is stopped | SIGTERM 走 ctx 取消，子进程整组被 kill(setProcGroup + killGroup)。 |
| 635 | issue | lsyncd not detecting file updates from Apache ActiveMQ Artemis | IN_MODIFY + MaxHold。 |
| 636 | issue | Optimize the delay queue | 聚合器用 map + 目录树吸收，无 O(n^2) 队列扫描（TestMillionPathsMemory）。 |
| 660 | issue | A directory is deleted, the deletion is omitted. | 目录删除事件被显式处理（TestDeleteForgetsChildren / TestMassDeleteOfNestedTreeDeletesEveryEntry）。 |
| 662 | issue | percpu memleak with multhreaded rsyncs | 无 C 版 per-cpu 分配；Go runtime 管理内存。 |
| 665 | issue | Lsyncd does not create a directory with the current date if you specify /dir/`date +%Y-%m-%d` | 不经 shell，不做变量替换；日期目录请用固定路径 + 外部脚本预建。 |
| 671 | issue | Lsyncd is causing memory leaks. | 同 #686。 |
| 678 | issue | Lsyncd ignores files that are not synced due to remote host errors | 失败批次 readd 回窗口重试，不会静默丢弃（TestFailedBatchIsReadded）。 |
| 685 | issue | Missing parent path leading to error 'Terminating since "insist" is not set.' | 目标端父目录不存在时自动 MkdirAll（copyFileAt 的 ErrNotExist 重试路径）。 |
| 686 | issue | Lsyncd is causing memory leaks. | watch 表在目录删除时清理，内存不随 churn 增长；51 万目录整进程 142–193MB（约 280–380B/目录）。 |
| 689 | issue | Reduce log verbosity when spawning shells | 日志节流 + 子进程输出按行收集。 |
| 691 | issue | side-effect of lsyncd in real-time log aggregator | 事件按路径折叠，日志有节流。 |
| 692 | issue | Normal, Retrying Move | 同 #247。 |
| 695 | issue | lsyncd get stuck when doing batch operations | 四层修复：目录删除按子树整体吸收；watcher 目录簿记改子节点索引（100 万目录下单次删除 44ms→229ns）；rsync 退出码 24「源文件已消失」不再当失败（批删期间它是常态，当失败会退避并触发全量重扫），`TestApplyTreatsExit24AsDelivered`；端到端专项（tmpfs 上 21 万目录 / 20 万文件整棵删除）62 万事件坍缩成 1 个批次、200 条变更，16s 收敛烧 9.4s CPU，`peak_pending` 5,206，`overflows`/`full_syncs` 为 0（§6.1）。 |
| 719 | issue | Error: Unknown exitcode Move | 退出码分类正确，未知码按失败重试。 |
| 725 | issue | Queue is broken, delay not at dpos 2.3.1 | Go 聚合器没有 dpos 概念；原 Lua 的 Queue.remove 中段删除 bug 已定位并复现（lsyncd/lsyncd.lua:606 把 nt.first 当成 nt.last 用）。 |
| 726 | issue | Cannot assign new key "alarm" to Delay | 没有 Delay 表。 |
| 728 | issue | lsyncd fails to sync large unpacked files | MaxHold 兜底 + 有界拷贝。 |
| 729 | issue | `inotifyMode` only for a specific sync | 事件掩码全局固定，不需要 per-sync inotifyMode。 |
| 737 | issue | on initial start, the files are not syncronized, only after saving indiviual files again, they … | adopt backfill 补发初始同步期间/之前创建的已有文件。 |
| 738 | issue | lsyncd falls into disaster doom loop when renaming files and folders, but only when run as root | rename 用 cookie 配对，不再降级成 delete+create 的循环。 |

## 规模与性能（10T / 100T） —— 8 条

不是修某个 bug 能解决的，而是重新设计的数据结构与调度：聚合器、watch 降级、prefetch、并发闸。DESIGN 第 3.2 / 6 节有完整推导。

| # | 类型 | 标题 | 结论 |
|---|---|---|---|
| 63 | issue | Slow response to sync when any file changing in a large file system | 事件驱动 + 聚合窗口，大文件系统只有变更路径产生工作。 |
| 202 | issue | Lsyncd causes load while monitoring large filesystem. | 聚合 + 事件驱动 + watch 降级；DESIGN §3.2 有专项分析。 |
| 263 | issue | lsyncd takes long time before it starts sync | 初始遍历用 prefetch 预读目录列表（21 万目录实测 126s→15.9s），并带进度日志。 |
| 387 | issue | sync file very slowly | prefetch + workers + 冷热缓存对比实测见 DESIGN §6。 |
| 597 | issue | Lsyncd can manage big data or 1TB data? | DESIGN 3.2/6 有 10T-100T 分析；1T 级真机实测见 6.1。 |
| 607 | issue | Quantify slow changing directories | prefetch 预读 + 冷热缓存对比实测（DESIGN 6）。 |
| 618 | issue | Too many files | watch 装不下时降级 + 周期全量；DESIGN 3.2 有 10T-100T 分析。 |
| 731 | issue | Use lsyncd sync 9T data between two nfs mount,many file and directory not sync | DESIGN 3.2/6；watch 降级 + 聚合 + prefetch。 |

## 重写本身即为答案 —— 8 条

问题根因是「用 Lua + 系统 Lua 库 + C 构建」这套形态。单静态二进制让它们整体消失。

| # | 类型 | 标题 | 结论 |
|---|---|---|---|
| 272 | issue | Binary Releases | 交付物就是单个静态二进制（CGO_ENABLED=0），无 Lua/rsync/ssh 依赖。 |
| 413 | issue | loading precompiled runner: runner: bad header in precompiled chunk | 没有 Lua runner，不存在预编译 chunk 兼容问题。 |
| 499 | issue | Static Building with lua or full static binary version (not dynamic binary) | 单静态二进制，无 Lua 依赖。 |
| 539 | issue | Need Template For lsyncd.service file | deploy/gsyncd.service 已提供。 |
| 541 | PR | add -onepass option, allowing exit after initial sync | -once 即等价能力。 |
| 569 | issue | Clear Linux: Error: error loading precompiled runner: runner: version mismatch in precompiled c… | 无预编译 runner。 |
| 614 | issue | about static compiler with built-in lua 5.3? | 静态二进制。 |
| 622 | issue | Error: error loading precompiled runner: runner: bad binary format (version mismatch) | 无 runner。 |

## 配置即可满足（不需改代码） —— 203 条

需求合理，但不需要新机制：rsync_extra、[sync.ssh]、excludes、delete、delay 等已有键能直接表达。

| # | 类型 | 标题 | 结论 |
|---|---|---|---|
| 1 | issue | Run lsyncd as different user? | 无 user 键；用 systemd 的 User= 或 root 启动。gsyncd 不做 setuid 切换。 |
| 5 | issue | lsyncd does not sync permissions and ownership | direct 保留 mode；属主需 root 加 rsync 后端 -a，gsyncd 不做 chown。文档答复。 |
| 6 | issue | SSH on another port | [sync.ssh] port 直接支持。 |
| 7 | issue | lsyncd does not allow to exclude directories | excludes 支持目录级排除。 |
| 8 | issue | Excluding-Problems | excludes 语义在 filter 包重写并有单测。 |
| 16 | issue | Make inotify events configurable | 事件掩码固定且已含 MODIFY/CLOSE_WRITE/ATTRIB/MOVE，无需用户配置；唯一影响语义的开关是 `close_write`，它不改掩码只改判定。 |
| 18 | issue | Add sub directory pattern exclusion | excludes 支持 **/ 形式的前缀与子目录模式。 |
| 23 | issue | regex support on 1.26 (see attached file) | excludes 是通配符（非 regex）；regex 需求用 rsync_extra 的 --filter 表达。 |
| 24 | issue | Know when parent directory rsync will be completed | status_file 有 pending/last_flush/applied 可直接观察。 |
| 26 | issue | Another binary/script with parameters | rsync_extra 可追加任意参数；自定义后端未支持。 |
| 28 | issue | Feedback for where in the sync queue we currently are | status_file 的 pending/batches 字段即队列进度。 |
| 38 | issue | Syncing one source to multple targets in lsyncd 2.0 | 同一 source 写多个 [[sync]] 即一源多目标。 |
| 44 | issue | Exclude list won't work correctly. | excludes 语义重写并有单测。 |
| 45 | issue | lsyncd is not compatible with use of rsync option --relative | gsyncd 传相对路径，--relative 不适用；需要时用 rsync 后端 + 自定义 target 结构。 |
| 51 | issue | incorrect '+' processing in exclude list | 排除模式语义重写。 |
| 56 | issue | Choose the sync directories by one name | 按 [[sync]] 的 name 区分。 |
| 66 | issue | Does -direct actually work? | direct 模式可用，有 TestDirectStartupMirrors 等覆盖。 |
| 67 | issue | default rsync option is broken ? | 默认参数在 README 配置参考中列全。 |
| 69 | issue | Change port SSH in config lsyncd | [sync.ssh] port。 |
| 74 | issue | add sshOpts | [sync.ssh] options 透传任意 ssh 参数。 |
| 76 | issue | feature request - add output for monitoring | status_file 提供监控输出。 |
| 81 | issue | Why only -lts option as argument to rsync | rsync_extra 可覆盖/追加参数。 |
| 87 | issue | rsyncOpts should be able to override "--delete" too | delete 键（true/false/startup/running）直接控制。 |
| 92 | issue | lsyncd ignores "--filter" command in rsyncOpts for rsyncssh (and maybe rsync) | rsync_extra 原样透传 --filter。 |
| 94 | issue | User and Group ownership not preserved | 需 root 且 rsync -a；direct 模式不 chown。 |
| 98 | issue | lsyncd does not mirror files that are inside symlink directories. | 不跟随符号链接是刻意设计（IN_DONT_FOLLOW）；需要时用 rsync 后端 + --copy-links。 |
| 100 | issue | -log Exec doesn't seem to work | exec 后端未实现；日志见 log_file。 |
| 104 | issue | lsyncd with ssh keys and other ssh options | [sync.ssh] identity_file 或 options。 |
| 105 | issue | Parsing of external file with option excludeFrom | 没有 exclude_from 键：把模式直接写进 `excludes` 数组。需要复用已有的模式文件时，`rsync_extra = ["--exclude-from=/etc/rsync.exclude"]` 对 rsync 后端生效，但 **watcher 侧的 filter 看不到那个文件**，被排除的路径仍会产生事件（只是 rsync 会跳过上传）—— 10T 树上这会白扫，所以推荐把模式内联。 |
| 114 | issue | lsyncd shutdown | SIGTERM/SIGINT 优雅退出。 |
| 129 | issue | how to use rsync before lsyncd | 先手动 rsync 再启动 gsyncd（配合 delete=false 或 checkpoint）。 |
| 132 | issue | rsyncOpts ignored?? | rsync_extra 生效，有 TestIncrementalArgsNeverDelete 等覆盖。 |
| 137 | issue | lsyncd+rsyncd : initial rsync failed : rsync syntax error during call ? | rsync 后端参数在 README 列全。 |
| 143 | issue | How to copy symlink files as actual files? | 符号链接不跟随，不复制内容；需要时用 rsync 后端 --copy-links。 |
| 144 | issue | Remove --delete option | delete=false。 |
| 162 | issue | how to add --password-file options to rsync | rsync_extra 里加 --password-file。 |
| 164 | issue | excludeFrom not working in 2.1.1 | 同 #105：无 exclude_from 键，模式内联进 `excludes`；`rsync_extra` 的 `--exclude-from` 只作用于 rsync 后端、不作用于 watcher。 |
| 166 | issue | rsync.archive not working | rsync_extra 可加 --archive。 |
| 170 | issue | default.rsyncssh ignores port | [sync.ssh] port 生效。 |
| 172 | issue | rsync parameters with ssh parameters under default.rsyncssh not working? | [sync.ssh] options。 |
| 173 | issue | rsync with ssh options not working | 同上。 |
| 175 | issue | logrotate | 内置 log_max_size/log_max_files 轮转；外部 logrotate 需 copytruncate。 |
| 176 | issue | Dont' replace variables when spawning a binary process. | 不经 shell，不做变量替换。 |
| 177 | issue | Delete files | delete 键。 |
| 180 | issue | No way to use ssh's identity_file option, ssh port, and get rsyncssh to work | [sync.ssh] port 与 identity_file。 |
| 184 | issue | Is it possible to set environment variables for forked rsync/ssh? | systemd Environment= 或进程环境继承。 |
| 185 | issue | Is it possible to suppy arguments to the initial rsync in default.direct? | 初始同步参数同 rsync_extra。 |
| 188 | issue | how to break full sync at startup? | -once 只做初始同步；启动中途取消用 SIGTERM。 |
| 191 | issue | 2.1.4 temp_dir flag | 无 temp_dir 键；staging 文件固定放目标目录内（.gsyncd-tmp-*），保证 rename 原子。 |
| 197 | issue | rsync --inplace argument | rsync_extra 里加 --inplace（注意与原子替换语义冲突）。 |
| 200 | issue | Lsyncd 2.1.4 with rsyncssh not preserving ownership and timestamps | 需 root + rsync -a。 |
| 201 | issue | Where to specify the sudo user in lsyncd configuration ? | 用 systemd User= 或 sudo 包装。 |
| 205 | issue | add rsync option --bwlimit | rsync_extra。 |
| 210 | issue | sync multiple source | 多个 [[sync]]。 |
| 214 | issue | -nodaemon and -direct options still result in rsync (2.1.5) | -nodaemon/-once 语义清晰，无 -direct 开关（模式在配置里）。 |
| 215 | issue | Bash options | 无 bash 选项，只有固定 CLI。 |
| 217 | issue | Add more options for SSH binary | [sync.ssh] binary 与 options。 |
| 218 | issue | Add option to start lsyncd as a specific user | systemd User=。 |
| 227 | issue | Lsyncd 2.1.4 with rsyncssh not preserving ownership | 需 root + rsync -a。 |
| 232 | issue | Exclude file extension? | excludes 支持 *.ext。 |
| 235 | issue | How could I setup lsyncd to make sync on specific time? | cron + -once。 |
| 236 | issue | How to make lsyncd sync files once a day | 同上。 |
| 237 | issue | rsyncssh: attributes are not synchronized on change | rsyncssh 属性需 -a；gsyncd 的 rsync_extra 默认含 -a。 |
| 241 | issue | No support for multiple targets | 多 [[sync]]。 |
| 242 | issue | Need support new parameters in rsync 3.1.0 | rsync_extra。 |
| 243 | issue | Support for --delete-excluded rsync option | rsync_extra 加 --delete-excluded。 |
| 248 | issue | Allow disabling of --safe-links rsync option in rsync config section | rsync_extra 里加 --no-safe-links 等覆盖默认参数。 |
| 249 | issue | Some files got stuck on backup location (they are not deleted ever) | delete=true 时 pruneDir 会删掉目标端多余项；delete=false 时保留是设计语义。 |
| 251 | issue | Lsync cant delete file on receiver | 删除需要目标端写权限；失败会在日志与 status 的 errors 里出现。 |
| 252 | issue | Add support for --existing rsync option | rsync_extra 里加 --existing。 |
| 258 | issue | --remove-source-files | rsync_extra 里加 --remove-source-files（注意与 delete 语义冲突，需自行确认）。 |
| 262 | issue | Lsyncd Not Excluding Swap Files | excludes 里加 *.swp / *~ 即可。 |
| 265 | issue | _extra not working | _extra 语义换成 rsync_extra / [sync.ssh] options，有 TestKeySpellingsAreEquivalent 守着别名。 |
| 268 | issue | Prevent sync trigger when time modified changes, but content is the same | 事件才是权威：同一 tick 内的写即使 mtime 看起来没变也会重拷（TestSameTickWriteStillCopies），这是刻意的取舍。 |
| 269 | issue | symbolic link folder not copied | 不跟随符号链接（IN_DONT_FOLLOW）；需要跟随时用 rsync 后端 + --copy-links。 |
| 271 | issue | One time sync | -once 只做初始全量后退出。 |
| 274 | issue | Cannot follow destination file with tail -f | 原子替换（rename）会让 tail -f 跟丢 inode，这是 direct 模式保证一致性的代价；需要跟读请用 rsync 后端 + --inplace。 |
| 277 | issue | --temp_dir wiil not working | staging 文件固定放目标目录内以保证 rename 原子，不提供 temp_dir。 |
| 282 | issue | logrotate breaks log sync | 内置 log_max_size/log_max_files 轮转；外部 logrotate 需用 copytruncate。 |
| 288 | issue | default.direct and nfs mounts | direct 在 NFS 上可用但 rename 语义依赖服务端；跨机同步建议 rsync 后端。 |
| 293 | issue | Is remote monitoring and syncing possible? | status_file 是 JSON，可被任意监控系统采集。 |
| 299 | issue | What about handling --backup option for rsync? | rsync_extra 里加 --backup/--backup-dir。 |
| 300 | issue | mechanism to notify of 'dirty' state | status_file 的 pending 字段即「脏」程度。 |
| 304 | issue | Allow for multiple sources to same target | 多个 [[sync]] 可以指向同一 target。 |
| 305 | issue | Lsyncd dose not startup sync jobs in the order specified in the config file | sync 按配置顺序启动，但初始同步并发进行（受 max_processes 限制）。 |
| 309 | issue | Documentation is missing "insist" | 无 insist 键：失败一律退避重试，不会因一次失败退出。 |
| 310 | issue | explicit support for --chmod option | rsync_extra 里加 --chmod。 |
| 311 | issue | Excluding directories not working as expected | excludes 的目录匹配语义在 filter 包重写并有单测（TestPatternCoversDescendants）。 |
| 313 | issue | Lsyncd non-root rsyncssh configurations | 非 root 需目标端权限；systemd User= 指定身份。 |
| 314 | issue | File that was deleted on target was not synced up by lsyncd | 单向镜像：目标端被外部删除的文件不会在增量同步中自动补回（事件只来自源端），要靠 `full_resync` 周期兜底或重启时的全量比对（`delete = true` 时）。 |
| 315 | issue | lsyncd attempting to rsync every 2 seconds | delay 控制批量，不会每 2 秒跑一次。 |
| 317 | issue | inotifywait exclude is not supported? | excludes 就是排除机制，不需要 inotifywait 的 exclude。 |
| 321 | issue | Group ownership not syncable. | 属主需 root + rsync -a；direct 模式不 chown。 |
| 325 | issue | How to write default.rsyncssh  ssh configuration | [sync.ssh] 的 port / identity_file / binary / options 在 README 列全。 |
| 327 | issue | Different computed options for initial vs update? | 初始与增量共用同一套参数；需要区分时用 checkpoint + 预置数据。 |
| 331 | issue | Rsync chown option | rsync_extra 里加 --chown。 |
| 333 | issue | Symlink content doesn't sync right away | 不跟随符号链接，所以符号链接内容不会同步；用 rsync 后端 --copy-links。 |
| 337 | issue | Delete doesnt use key | [sync.ssh] identity_file 对删除同样生效（删除也是同一条 ssh 通道）。 |
| 339 | issue | network mounted directory | 网络挂载目录作为 source 可用，但事件依赖本地内核；建议 rsync 后端 + 周期 full_resync。 |
| 347 | issue | --temp-dir with share rsyncd | staging 固定放目标目录，不提供 temp_dir。 |
| 354 | issue | misbehaving with "--remove-sent-files"  or "--remove-source-files" | rsync_extra 里加 --remove-source-files。 |
| 356 | issue | Add options for "--omit-dir-times" and "--omit-link-times". | rsync_extra 里加 --omit-dir-times。 |
| 357 | issue | default.rsync pass source directory | source 就是被镜像的目录本身。 |
| 358 | issue | Why is copy_links enabled by default? | 不跟随符号链接是刻意设计（避免环与越界）；可用 rsync 后端覆盖。 |
| 360 | issue | How should I  edit the configuration file to sync multi target dir with one source dir? | 多个 [[sync]]。 |
| 363 | issue | Symbolic link config does not behave as expected | 符号链接策略见 README。 |
| 366 | issue | Modify rsync command? | rsync_extra。 |
| 369 | issue | Delete remote file with key based auth | [sync.ssh] identity_file。 |
| 374 | issue | Remove default --ignore-files and --exclude options | 无默认排除项，excludes 为空即不排除任何东西。 |
| 376 | issue | Synchronizing multiple directory in multiple server | 多个 [[sync]] + 每个可指不同 target。 |
| 382 | issue | How to force lsyncd use specific private ssh key? | [sync.ssh] identity_file 或 options 里加 -i。 |
| 388 | issue | one source, two (or more) targerts? | 同 #38：一源多目标写多个 [[sync]]。 |
| 389 | issue | Sync with git repository | git 仓库就是普通目录树；.git 可用 excludes 排除。 |
| 394 | issue | Simultaneous replicate files with multiple targets | 多个 [[sync]] 即多目标并发，受 max_processes 限流。 |
| 396 | issue | rsyncssh identity flag not working | [sync.ssh] identity_file。 |
| 399 | issue | Using sync.excludeFrom prevent sync | 无该键，不会出现「排除文件读坏导致整个 sync 不工作」这条失败路径；语义在 filter 包重写并有单测（`TestFastPathKeepsDescendantRule`）。 |
| 400 | issue | lsyncd -direct doesn't preserve mode and ownership | direct 保留 mode 与 mtime；属主需 root + rsync 后端。 |
| 402 | issue | lsyncd-2.1.5 rsyncssh ignores ssh _extra | [sync.ssh] options 透传。 |
| 403 | issue | Preserving owner:group from source to target? | 需 root + rsync -a。 |
| 405 | issue | [question] Lsyncd and Logrotate | 内置轮转，不依赖 logrotate。 |
| 409 | issue | Add support for rsync's --copy-dirlinks | rsync_extra 里加 --copy-dirlinks。 |
| 414 | issue | How to remove source files _and_ folders? | rsync_extra 里加 --remove-source-files（目录用 --delete 组合）。 |
| 425 | issue | Can lsyncd sync two single files? | source 是目录；单文件用其父目录 + excludes 收窄。 |
| 430 | issue | lsyncd 2.1.6 doesn't track changes of symlinked files | 不跟随符号链接。 |
| 433 | issue | _extra suffix argument not work | rsync_extra 的写法，不解析后缀。 |
| 440 | issue | lsyncd logging | log_level 可调。 |
| 442 | issue | _extra can not use empty param | rsync_extra 是数组，空串会被原样传递，不建议这么用。 |
| 444 | issue | Alternative destinations | 多个 [[sync]]。 |
| 445 | issue | exclude not working | excludes 语义有单测覆盖。 |
| 446 | issue | Multiple source/path --> target/path configurations? | 多个 [[sync]]。 |
| 448 | issue | Why using IN_DONT_FOLLOW by default? | IN_DONT_FOLLOW 是为了避免符号链接环与越界；需要跟随时用 rsync 后端。 |
| 449 | issue | Can't capture stdout/stderr | 子进程输出被捕获进日志（带 splitLines 上限保护）。 |
| 450 | issue | Sould I set up a cron job for auto syncing? | cron + -once 是支持的用法。 |
| 454 | issue | it would be great if "recursive" exclude worked | excludes 支持目录级与 **/ 递归模式。 |
| 457 | issue | "identityFile" option in ssh does not work & Outdated version 2.1.5 on Debian 8 | [sync.ssh] identity_file。 |
| 464 | issue | Preserve ownership using default.direct | 同 #400。 |
| 468 | issue | Add --force and --partial rsync options | rsync_extra 里加 --force/--partial。 |
| 469 | issue | Errors not visible to ansible. | 退出码 + status_file 的 errors/last_error 可被 ansible 读取。 |
| 473 | issue | Add support for --ignore-existing rsync option | rsync_extra 里加 --ignore-existing。 |
| 479 | issue | Is there a way to to ignore non-zero status code from spawn? | retries / retry 退避由引擎统一处理，不需要外部忽略非零码。 |
| 480 | issue | Lsync multiple directories | 多个 [[sync]]。 |
| 481 | issue | Sane Defaults | 默认值在 README 配置参考里逐项列全。 |
| 482 | issue | how to trans rsync commands to lsyncd commands? | rsync_extra 直接写 rsync 参数。 |
| 491 | issue | My exclude list seems not to work | excludes 语义重写并有单测。 |
| 494 | issue | How to run .lua config on system startup | systemd unit 已提供（deploy/gsyncd.service）。 |
| 504 | issue | how "ssh -o StrictHostKeyChecking=no" in _extra | [sync.ssh] options。 |
| 507 | issue | Is there is way to know if particular set of files are finished syncing? | status_file 的 pending 归零即「这一批完成」。 |
| 508 | issue | Lsync synchronization one time will not synchronize | -once。 |
| 510 | issue | where  does the "--filter" should be configured? | rsync_extra 里加 --filter。 |
| 521 | issue | Multiple hosts in a group | 多个 [[sync]]。 |
| 525 | issue | Rsync option "--files-from=" not working with Lsyncd. | 不用手写 --files-from=，gsyncd 自己用 --files-from=- --from0 传。 |
| 526 | issue | Multiple excludeFrom entries? Combining excludeFrom with exclude? | `excludes` 是数组，可任意多条，天然合并；无 excludeFrom 键（直接写进配置）。 |
| 527 | issue | Chown not working | 需 root + rsync -a。 |
| 532 | issue | can i lsyncd different source to destination with "for _, site in ipairs(sites) do" | 多 [[sync]] 或配置文件模板生成。 |
| 534 | issue | No documentation on how to specify port for default.rsync or which port is used | [sync.ssh] port。 |
| 542 | issue | Cannot delete non-empty directory | 目标端非空目录用 RemoveAll，不会被非空卡住。 |
| 547 | issue | lsyncd doesn't appear to work with 'sudo rsync' | sudo rsync 用 rsync_extra 或 systemd 身份。 |
| 549 | issue | lsyncd stats | status_file。 |
| 550 | issue | Exclude is not working | excludes。 |
| 551 | issue | Remote sync not using specified certificate file | [sync.ssh] identity_file 或 options。 |
| 554 | issue | Automatically trigger synchronization | cron/timer + -once。 |
| 557 | issue | add delete to commandline option. | 命令行无 -delete，配置里 delete 键。 |
| 558 | issue | I don't quite understand "delay" option | delay 是「静默多久后开始传」；README 有专门说明。 |
| 560 | issue | LSYNC as non root account | systemd User=。 |
| 563 | issue | lsyncd does not log into syslog | 日志写文件或 stderr；不写 syslog（可用 journald 接管）。 |
| 565 | issue | File ownership and permission problem | 属主/权限需 root + rsync -a。 |
| 567 | PR | Expose rsync's copy_unsafe_links | rsync_extra 里加 --copy-unsafe-links。 |
| 570 | issue | ssh : binary option has no effect | [sync.ssh] binary。 |
| 571 | issue | How to specify port if i want to use native rsync ? | [sync.ssh] port。 |
| 574 | issue | Unknown --usermap name on receiver: oracle | rsync_extra 里加 --usermap。 |
| 581 | issue | multiple servers to multiple servers sync. | 多个 [[sync]]。 |
| 588 | issue | Lsyncd 2.2.2 ownership and permission issue | 需 root + rsync -a。 |
| 593 | issue | sync when files are removed on destination | delete=true 时目标端被删的文件会按源端补回；反向感知请用双向工具。 |
| 595 | PR | allow deleting excluded files with rsync | rsync_extra 里加 --delete-excluded。 |
| 599 | issue | Logs to syslog even when logfile is configured | 不写 syslog。 |
| 600 | issue | Add --max-size | rsync_extra 里加 --max-size。 |
| 603 | issue | Can't make rsync filter xattrs | rsync_extra 里加 --xattrs。 |
| 608 | issue | Add --copy-unsafe-links to rsync options | rsync_extra 里加 --copy-unsafe-links。 |
| 611 | issue | Add random delay to initiate SSH control connection to prevent Overloading receiving side | [sync.ssh] options 可加连接参数；随机延迟需外部控制。 |
| 619 | issue | encoding, line termination problem when _extra calls for files | 路径按字节传，无编码转换。 |
| 624 | issue | reverting to rsync optimized version with rsync 3.2.3? | rsync_extra 原样透传。 |
| 632 | issue | lsyncd Terminating since "insist" is not set | 无 insist 键：失败一律退避重试。 |
| 634 | issue | sync interval | delay 即同步间隔语义。 |
| 639 | issue | Ownership not maintained | 属主需 root + rsync -a。 |
| 641 | issue | Config example to sync only directories | excludes 可表达。 |
| 648 | issue | ssh not use root | systemd User= 或把 target 写成 `user@host:/path`。 |
| 659 | issue | File owner is not set correctly when synchronizing a large number of files. | 属主需 root + rsync -a。 |
| 669 | issue | Sync files over rsyncssh using specific user | 把 target 写成 `user@host:/path`。 |
| 672 | issue | perm, owner not picked up - centos 9 | 同 #639。 |
| 674 | issue | synchronize specific files | excludes 收窄。 |
| 676 | issue | default.rsyncssh cannot sync to multple targets simultaneously | 多个 [[sync]] 指向同一目标主机。 |
| 677 | issue | how to filter exclude folder? | excludes 支持目录模式。 |
| 679 | issue | Lsyncd does not start if crontab is used | systemd unit 已提供，不依赖 crontab。 |
| 702 | issue | 'chown' when globbed skips files | rsync_extra 里加 --chown。 |
| 706 | issue | How to transfer files after sensing no changes | -once + timer。 |
| 708 | issue | excludeFrom doesn't work... | 无 excludeFrom 键；`excludes` 的匹配语义由 filter 包单测钉住（锚定、`**`、目录模式、取反）。 |
| 716 | issue | SSH option incorrectly parsed | [sync.ssh] 解析重写，有 TestRSHEndpoint 等覆盖。 |
| 717 | issue | Enable --backup-dir | rsync_extra 里加 --backup-dir。 |
| 720 | issue | Add rsync --mkpath argument | rsync_extra 里加 --mkpath。 |
| 727 | issue | Question: what is the default rsync command lsyncd actually run | rsync 后端默认参数在 README 列全。 |
| 736 | issue | Can I run lsyncd as non-root | systemd User=。 |
| 740 | issue | excludeFrom not working | 无 excludeFrom 键，写进 `excludes` 数组即可；见 #105 对 `--exclude-from` 只覆盖 rsync 后端的说明。 |
| 744 | issue | Why can't the logs be output to the specified file? | log_file 需可写目录；目录会自动创建。 |

## 尚未实现（已记录在 DESIGN 第 5 节） —— 71 条

确认是真实需求，但不在本轮范围内。每条都写清为什么没做、现在用什么替代。

| # | 类型 | 标题 | 结论 |
|---|---|---|---|
| 11 | issue | Implement support for fsnotify / entire system backup | 未实现 fsnotify 整机备份；DESIGN §5 记录。 |
| 25 | issue | How to skip initial rsync | 无 skip-initial 开关；等价做法是目标端已存在 + delete=false。DESIGN §5 记录。 |
| 64 | issue | Request pre and post rsync shell script | hook（onStartup/onMove/onDelete 等 Lua 回调）未实现，DESIGN §5 记录。 |
| 71 | issue | stop looking at new folder | 无暂停/恢复单个 sync 的接口；DESIGN §5 记录。 |
| 95 | issue | Bi-directional sync (possibly with unison?) | 双向同步不做，DESIGN §5 记录。 |
| 102 | issue | Add support for includes | 单文件 TOML，无 include；DESIGN §5 记录。 |
| 127 | issue | Allow discarding of event lists | 无 Lua 侧事件列表可丢弃（没有 Lua 后端）。 |
| 128 | issue | Make source paths and event types available from an event list | 同上，无 Lua 事件对象。 |
| 133 | issue | How to run local command | 无 hook，无法在同步前后跑本地命令。 |
| 134 | issue | How to run local command | 同上。 |
| 136 | issue | Bidirectional synchronization | 双向同步不做。 |
| 142 | issue | Re-sync file that is deleted on target | 同 #314：目标端被外部删除不会自动补回；用 full_resync 周期兜底，或 restart 时走全量比对。 |
| 178 | issue | 2 way lsyncd<->lsyncd timestamp problem | 双向不做。 |
| 181 | issue | Feature request: per-sync log settings | per-sync 日志级别未实现（全局 log_level）。 |
| 183 | issue | onAttrib not accepted | 无 Lua 回调（onAttrib 等）。 |
| 208 | issue | how to execute commands before/after every sync operation | 无 hook。 |
| 234 | issue | Adding onChanged event. | 无 onChanged 回调。 |
| 260 | issue | post command after sync | 无 hook（onStartup/onDone 等）；DESIGN §5 记录。 |
| 264 | issue | an include statement | 单文件 TOML 配置，无 include。 |
| 267 | issue | Some way of defining sync priority for files in a directory | 无按文件优先级调度；max_processes 只能限总并发。 |
| 275 | issue | Multiple config files | 无多配置文件/conf.d；DESIGN §5 记录。 |
| 276 | issue | How to turn off the initial sync | 无 skip-initial 开关；等价做法是目标端预置数据 + delete=false + checkpoint。 |
| 284 | issue | Switch over to fanotify | fanotify 需内核 5.9+ 与 CAP_SYS_ADMIN，不做；DESIGN §5 记录。 |
| 287 | issue | Lsyncd 2.1.4+ : Perform an action based on the failure type of a default.rsync (or any other de… | 无按失败类型分派的回调。 |
| 297 | issue | on startup / on shutdown events | 无 on startup/shutdown hook。 |
| 303 | issue | two way sync of data using lsyncd | 双向同步不做。 |
| 308 | issue | Programmatically Add and/or Remove Slaves | 无运行时增删 sync 的接口。 |
| 335 | issue | How to delete source file only after all targets are synced? | 无「等所有目标都同步后再删源」；可用 rsync_extra 的 --remove-source-files 自行组合。 |
| 338 | issue | rsync onCompleted | 无 onCompleted 回调。 |
| 342 | issue | Is lsyncd useful for bidirectional synchronization ? | 双向不做。 |
| 361 | issue | lsyncd doesn't allow use of "/dev/stdin" or "-" for config filename | 不支持 /dev/stdin 作为配置路径。 |
| 362 | issue | How to distribute a custom backend | 无插件式自定义后端。 |
| 368 | issue | Possible to rsyncssh with a remote source or rsync pull? | 只做 push（源在本机），不做 pull。 |
| 373 | issue | Guidance on 2 way bidirectional syncing with lsynd? | 双向不做。 |
| 392 | issue | feature request: exit after idle for some time | 无「空闲 N 秒后退出」；可用 -once 或外部 systemd timer。 |
| 395 | issue | Call a script only when whole rsync process is complete | 无 rsync 完成回调。 |
| 404 | issue | Handling for hard links? | 不处理硬链接（DESIGN §5 记录）；direct 模式会复制成独立文件。 |
| 416 | issue | Use ssh ControlMaster/ControlPersist as default. | 不内置 ControlMaster；可在 [sync.ssh] options 里加 -o ControlMaster=auto。 |
| 418 | issue | Custom signal handlers. | 无自定义信号处理。 |
| 432 | issue | Support for suspending operations | 无暂停/恢复接口。 |
| 435 | issue | Parameter "onModify" unknown. | 无 onModify 回调。 |
| 441 | issue | lsyncd conf.d solution actually works or not? | 无 conf.d；DESIGN §5 记录。 |
| 451 | issue | Using Excludes with Shell Scripts? | 无 shell hook。 |
| 455 | issue | how to spown  event list twice | 无事件列表 API。 |
| 462 | issue | desktop notifications | 无桌面通知。 |
| 489 | issue | Question: If source is in the rsync protocol | 不支持 rsync 协议作为源。 |
| 490 | issue | Lsyncd 3 proposal | Lsyncd 3 提案里的 systemd/现代化项已部分落地；其余见 DESIGN §5。 |
| 493 | issue | Does anyone use the onModify = "some shell command" syntax of Lsyncd? | 无 shell hook。 |
| 505 | issue | Missing configtest | -check 已提供配置校验；但无 dry-run。 |
| 509 | issue | How to configure the 'include' ? | 无 include。 |
| 515 | issue | Dynamically changing the synchronization targets? | 运行时改目标需重启进程。 |
| 517 | issue | Replication master-master problem | 双向不做。 |
| 528 | issue | Can this be used in the other direction -- to monitor a remote? | 只做 push。 |
| 546 | issue | Clarification for a Bi-directional Workstation <-> Server backup / restore approach | 双向不做。 |
| 555 | issue | How to stop synchronising a folder. | 运行时停止单个 sync 需重启。 |
| 561 | issue | How to stop accepting rsync push from slave | 不支持被 push。 |
| 573 | issue | syncd (Live Syncing Daemon) synchronizes Remote source with local directories targets | 只做 push。 |
| 578 | issue | omit syncing of directory structure | 无法只同步目录结构；可用 excludes 反选。 |
| 589 | issue | multiple inlet.addExclude | 无 inlet API。 |
| 606 | issue | Can't use on remote system | 只做 push。 |
| 640 | issue | Lsync from remote to local | 只做 push。 |
| 645 | issue | Support all `.gitignore` exclusion patterns | 只支持通配符，不支持完整 .gitignore 语义；DESIGN 5 记录。 |
| 653 | issue | ~rap & act on errors with a spawned rsync process? | 无 hook。 |
| 654 | issue | Trap errors on the spawned rsync processes? | 同上。 |
| 658 | issue | fanotify got fixed to report moves and could be used now instead of inotify | fanotify 不做。 |
| 666 | issue | rsync not working in two way lsyncd configuration | 双向不做。 |
| 690 | issue | How to setup bidirectionnel syncing btw 2 folders ? | 双向不做。 |
| 693 | issue | Parameter "onCreate" unknown. ( if this is not a typo add it to checkgauge ) | 无 onCreate 回调。 |
| 701 | issue | Can I configure log output separately for each setting? | per-sync 日志未实现。 |
| 714 | issue | How to ignore or discard event | 无事件丢弃 API。 |
| 715 | issue | FR: Passing command line arguments to lua interpreter | 无 Lua 解释器。 |

## 构建 / 打包 / 平台问题 —— 222 条

C + autotools + Lua 头文件 + 特定发行版/内核/桌面平台的产物。Go 重写后不适用。

| # | 类型 | 标题 | 结论 |
|---|---|---|---|
| 2 | issue | The lsyncd.conf.xml.5 man page gets installed in man1 | C/autotools 打包问题，Go 重写后不存在。 |
| 4 | issue | libxml2-dev dependency not checked by configure script | C 依赖检查，已无此依赖。 |
| 10 | issue | lsyncd fails on rsync versions not supporting -d option | rsync 版本差异，Go 直接拷贝不走 -d。 |
| 17 | issue | Segfault with trunk (r98) | C 版本段错误，历史版本问题。 |
| 19 | issue | v1.33,when setting <no-daemon/> will cause Segmentation fault | 历史版本段错误。 |
| 22 | issue | Segfault on ubuntu 10.04 x64 with latest 1.33 (ok with 1.26) | 历史段错误。 |
| 27 | issue | does FreeBSD can install lsyncd ? | FreeBSD 平台，不在支持范围。 |
| 29 | issue | warning at compilation | 编译警告，Go 重写后无。 |
| 32 | issue | Please support Kqueue | kqueue 不在范围（Linux only）。 |
| 33 | issue | compilation error of lsyncd2.beta1 | 编译错误。 |
| 34 | issue | lsyncd 2.0beta1 - rsync doesnt behave like I supposed to. | beta 版本行为差异。 |
| 35 | issue | [PATCH] SUSE sysconfig/proxy config support | 发行版打包。 |
| 37 | issue | compilation warnings and lbash copy errors | 编译警告。 |
| 42 | issue | lsyncd 2.0 crash | 2.0 崩溃。 |
| 50 | issue | minor spelling issues | 拼写。 |
| 53 | issue | Doesn't compile properly under Interix | Interix 平台。 |
| 55 | issue | [PATCH] lsyncd has executable stack | 可执行栈是 C 链接属性。 |
| 59 | issue | common typo error should be logged [rsyncOpts] | 文档拼写。 |
| 60 | issue | Documentation Enhancements | 文档。 |
| 62 | issue | Dead code removal. | C 死代码。 |
| 68 | issue | better docs for basic setup examples ? Config File not found error (example) | 文档。 |
| 70 | issue | compilatio errors on SLES 10. | SLES 编译。 |
| 72 | issue | --docdir does not change exampledir accordingly | autotools。 |
| 82 | issue | rsyncOps in documentation should be rsyncOpts | 文档拼写。 |
| 83 | issue | lsyncd crashes when it gets a notification from kernel | 历史崩溃。 |
| 86 | issue | Documentation | 文档。 |
| 88 | issue | Ubuntu installation ./configure finding lua | 找 Lua。 |
| 89 | issue | lsyncd on arm system not finding getConfig() | ARM 找 getConfig。 |
| 91 | issue | Compilation Errors on Solaris 11 Express | Solaris。 |
| 103 | issue | Lion version | Lion 平台。 |
| 108 | PR | Fixed typo. | PR 拼写。 |
| 109 | PR | Fixed a typo on one of the examples | PR 拼写。 |
| 111 | issue | problem with lsyncd 2.0.5 and 2.0.6 | 2.0.5/2.0.6 历史。 |
| 116 | PR | correct small syntax error in default-direct.lua | PR 语法。 |
| 118 | issue | [2.0.6 and Git HEAD] Link errors for ./configure --with-runner=[..] | 链接错误。 |
| 119 | issue | [2.0.6 and Git HEAD] Dead line "default.o: default.c" in Makefile.am? | Makefile 死规则。 |
| 120 | issue | [Git HEAD] asciidoc 8.6.7 has manpage.xsl at /usr/share/ rather than /etc | asciidoc 路径。 |
| 123 | issue | how to rerun make when i changed lsyncd.lua | C 构建。 |
| 125 | issue | Doc-Problem (?): Exclusion Lists | 文档。 |
| 130 | issue | LUA problem during configuration | Lua 配置。 |
| 135 | PR | adding spaces to output | PR 输出空格。 |
| 138 | issue | Freebsd version or install to FreeBSD | FreeBSD。 |
| 139 | issue | LUA error configuring on lion | Lua on Lion。 |
| 141 | issue | Compiling error debian | 编译。 |
| 145 | issue | compile error on Scientific Linux 6.2 | 编译。 |
| 146 | issue | Lua 5.2 support | Lua 5.2。 |
| 147 | PR | Lua 5.2 support | PR Lua 5.2。 |
| 148 | PR | Lua 5.2 support | PR Lua 5.2。 |
| 149 | PR | sample nagios monitoring scripts | PR nagios 脚本。 |
| 151 | PR | Fix typo in name of Lua global table lysncd->lsyncd | PR typo。 |
| 152 | PR | Simplify register functions | PR。 |
| 153 | PR | Check for presence of a2x (from asciidoc) in configure and use path in Makefile | PR autotools。 |
| 154 | PR | Check for presence of lua and luac in configure and use path in Makefile | PR autotools。 |
| 155 | PR | Add manpage to CLEANFILES | PR Makefile。 |
| 156 | PR | Modernize AM_INIT | PR autotools。 |
| 157 | PR | Try harder to find matching Lua program/library pairs | PR Lua 探测。 |
| 158 | PR | Check whether Lua library was compiled with compat support | PR Lua compat。 |
| 159 | issue | Missing Manual | 文档。 |
| 160 | PR | fix for non working rsync options | PR rsync 选项。 |
| 163 | issue | Parameter "rsync.1" unknown. (if this is not a typo add it to checkgauge) | 参数名拼写检查（C 版 checkgauge）。 |
| 165 | PR | Add excludeFrom to default-rsync checkgauge for #164 | PR。 |
| 167 | issue | lsyncd exits prematurely with "Internal fail, fsevents, no path." on OS X 10.8.2 | macOS fsevents。 |
| 169 | PR | Make default behavior preserve ownership and perms | PR 属主默认值。 |
| 179 | issue | Wiki update | wiki。 |
| 182 | issue | 2.1 Layer 3 Config page error | 文档。 |
| 187 | issue | Parameter "rsyncOpts" unknown. | 参数名检查。 |
| 189 | PR | README.md: fix spelling errors | PR。 |
| 190 | PR | README.md: update link to latest manual | PR。 |
| 192 | issue | lsyncd segfault.  lsyncd.lua:1599: attempt to compare string with number | Lua 比较崩溃。 |
| 193 | issue | FreeBSD Problem compiling with pkgng | FreeBSD。 |
| 195 | PR | Adding --timeout to default.rsync | PR --timeout。 |
| 198 | issue | Build on Mac OS X 10.7.5 fails | macOS 构建。 |
| 199 | issue | cannot compile lua5.2/5.1.5 for lsyncd | 编译 Lua。 |
| 203 | PR | Fix documentation spelling bits, and a few links | PR。 |
| 204 | issue | fsevents event type missing | macOS fsevents。 |
| 213 | PR | fix the size of the buffer allocated for pipemsg | PR。 |
| 216 | issue | examples/lpostcmd.lua fails checkgauge test | 示例脚本。 |
| 219 | issue | Doesn't find lua on debian 6 | 找 Lua。 |
| 221 | PR | Fixes for parsing excludes file | PR 排除解析。 |
| 222 | issue | lsyncd 2.4.1 or 2.5.1  error message Lua library needs to be compiled with compat support | Lua compat。 |
| 224 | PR | Fixed a bug with critical error on "0" exitcode | PR exitcode。 |
| 228 | issue | Doesn't find LUA on redhat 6 | 找 Lua。 |
| 229 | issue | exclude-directive on layer 3 | Layer 3 文档。 |
| 231 | PR | Link to official BindFS homepage in README | PR。 |
| 233 | issue | Install error when parse lsyncd.1.txt | 安装脚本。 |
| 238 | PR | Fix logging message | PR 日志。 |
| 239 | PR | Shutdown sigterm | PR 信号。 |
| 240 | issue | Not detecting Lua on MacOs X | 找 Lua。 |
| 244 | issue | Error: unknown event(831) / event(2125) in fsevents. | fsevents。 |
| 245 | issue | configure option to disable asciitools | autotools。 |
| 246 | PR | Fix for older kernels to make sure O_CLOEXEC is defined | C 构建细节，O_CLOEXEC 由 Go runtime 处理。 |
| 253 | PR | Add missed comma. | PR 语法修正。 |
| 255 | PR | Typo in print format. | PR 格式字符串。 |
| 257 | issue | Macos support: migrating to Framework FSEvents ? | macOS 平台，不在支持范围。 |
| 261 | PR | autogen for MacOSX | PR autotools。 |
| 270 | issue | ubuntu, lsync 2.0.4 and rsyncOps/rsyncOpts | 参数名拼写，旧版 checkgauge。 |
| 278 | issue | Adding excludeFrom and exclude to Custom Configuration With checkgauge | 旧版 checkgauge 参数表。 |
| 279 | PR | fix path to man page source | PR 路径。 |
| 280 | issue | Typo in Github description | typo。 |
| 281 | PR | Fix usage of settings in Lua 5.1 | PR Lua 5.1。 |
| 283 | PR | Clarify the disclaimer. | PR 文案。 |
| 285 | PR | fix lost file | PR 丢文件。 |
| 286 | issue | Manual missleading | 文档。 |
| 289 | PR | Add rsync.inplace option | PR inplace。 |
| 291 | issue | Executing rsync with wrong arguments on OS X Yosemite | macOS 平台。 |
| 307 | PR | Fix lpostcmd example | PR 示例。 |
| 318 | issue | Out-of-tree build failure for "doc" step. | 构建。 |
| 319 | issue | Incorrect man page install on OS X 10.3.3 | 打包。 |
| 322 | issue | Hypotetical Scenarios | 讨论帖。 |
| 323 | PR | Devel | PR。 |
| 326 | issue | rsync exclude documentation incorrect | 文档。 |
| 329 | issue | Can't compile | 编译。 |
| 330 | PR | Two smaller fixes | PR。 |
| 332 | issue | LSYNCD with iconv | iconv 编码。 |
| 334 | PR | Adding symlink support | PR。 |
| 341 | issue | Issues compiling under Mac OS X 10.5 | OS X 10.5。 |
| 344 | issue | protect_args crashes on Mac OSX 10.11 | macOS。 |
| 346 | issue | Error: There can only be one config file in command line. | 命令行解析。 |
| 349 | PR | fix: direct mode allows injecting unauthorized filesystem operations | PR。 |
| 352 | issue | relative ^d.targetPathname? | 文档。 |
| 355 | PR | Correct typo | PR。 |
| 359 | issue | lsyncd.service doesn't copy over file changes | 旧 unit 文件；deploy/gsyncd.service 已重写。 |
| 364 | issue | relative path ^pathname starts with a slash !? | 旧版路径处理。 |
| 365 | PR | Fix: Remove extra colon from example in README.md | PR。 |
| 372 | issue | rc value in embedded default.lua code is nil when there is custom handler with custom exitcodes… | Lua 回调退出码。 |
| 375 | issue | error: 'struct dirent' has no member named 'd_type' | 平台头文件。 |
| 377 | issue | fatal error: signal.h: No such file or directory | 平台头文件。 |
| 378 | PR | Fix typo: Exlcusions => Exclusions | PR。 |
| 379 | PR | default.collect: Print target | PR。 |
| 380 | PR | Fix typo: width => with | PR。 |
| 385 | PR | Improve Config Layer 2 manual page | PR 文档。 |
| 390 | issue | Mac10.11.5 runing lsyncd warn | macOS。 |
| 391 | issue | Lsyncd Error :There can only be one config file in command line. | 命令行。 |
| 393 | issue | Lua 5.3 Support | Lua 版本。 |
| 397 | issue | Example: Config Layer 2: Advanced onAction  problem | Layer 2 示例。 |
| 398 | PR | Updated lbash.lua and lrsync.lau to remove the equals (=) | PR。 |
| 408 | issue | Does not link | 构建。 |
| 410 | issue | Error/bug in layer 1 documentation | 文档。 |
| 411 | issue | Does not work on macOS 10.12.1 | macOS。 |
| 412 | PR | also applicable for commands BEFORE sync | PR。 |
| 417 | issue | Actualize layer 1 documentation | 文档。 |
| 419 | issue | ubuntu 16.04 systemd issue | systemd 打包。 |
| 420 | PR | Docs: Correct spelling of "insist" | PR。 |
| 421 | PR | Docs: Add "archive" to table of rsync options | PR。 |
| 424 | issue | Older manual versions | 文档版本。 |
| 428 | issue | Cannot access /dev/fsevents monitor | macOS。 |
| 429 | issue | How to uninstall or reinstall lsyncd? | 打包。 |
| 434 | issue | lsyncd 2.2 compile errors undfined reference | 编译。 |
| 436 | PR | Build with Docker | PR docker。 |
| 437 | PR | Build with docker | PR。 |
| 447 | issue | Cannot make - lua not found | 找 Lua。 |
| 456 | PR | "pathname part" -> "pathname segment" | PR 文档。 |
| 459 | PR | config file url | PR。 |
| 463 | PR | Add example config for syncing with an AWS S3 bucket | PR 示例。 |
| 465 | PR | Created proper systemd service description of lsyncd | PR。 |
| 471 | issue | lsyncd binary name not very handy | 命名偏好。 |
| 477 | PR | Fix documented default value of `nodeamon` flag | PR 文档。 |
| 483 | PR | Added handling of XATTR changes as ‘Attrib’-events. | PR xattr。 |
| 484 | PR | Replace `true` -> `BOOL` | PR 类型。 |
| 487 | issue | Lsyncd is not fully support Lua 5.3 | Lua 版本。 |
| 492 | issue | Version 2.2.3 fails to find lua on macOS | 找 Lua。 |
| 495 | issue | Lsyncd service failure | 旧 unit 文件。 |
| 498 | issue | doesn't compile on CentOS 7.4.1708 | 编译。 |
| 500 | PR | Fix for non-versioned lua not being searched in Cmake | PR cmake。 |
| 501 | issue | An "make" error Arhlinux | 编译。 |
| 502 | issue | Building on Amazon Linux 2 | 编译。 |
| 511 | issue | Version 2.2.3 fails to find lua on CentOS 6.9 | 找 Lua。 |
| 512 | issue | make error | 编译。 |
| 514 | issue | lsyncd won't execute | 平台。 |
| 518 | issue | Adding docker based deployment | docker。 |
| 523 | PR | Fix errors re exclude & excludeFrom (#1) | PR。 |
| 529 | PR | Update index.md | PR 文档。 |
| 535 | issue | lsyncd: /usr/lib/x86_64-linux-gnu/liblua5.1.so.0: no version information available (required by… | 动态链接警告，静态二进制后不存在。 |
| 536 | PR | Edited index.md for clarity and style. | PR 文档。 |
| 538 | PR | Fixed typo at readme.md | PR。 |
| 544 | issue | Building Issue | 构建。 |
| 548 | issue | fsevents segfault on OSX | macOS。 |
| 553 | issue | Error: unknown option command line option -runner | 命令行。 |
| 559 | PR | Say default for delay; better explanations of using rsync parameters | PR 文档。 |
| 568 | PR | version format in lua.h changed | Lua 头文件。 |
| 579 | issue | start message in log reports exclusions in non-deterministic order | 日志顺序；现在排除列表按配置顺序打印。 |
| 583 | PR | Enable ssh._extra args | PR。 |
| 584 | issue | *suggest* mention path of lsyncd.conf.lua | 文档。 |
| 587 | issue | Only sync when launch after upgrade to 10.15.1 | macOS。 |
| 594 | issue | Centos 8 | CentOS 8 打包。 |
| 596 | issue | Parameter "rsync.backup" unknown. (if this is not a typo add it to checkgauge) | 参数名。 |
| 598 | issue | Error: error preparing /etc/lsyncd/lsyncd.conf.lua: Parameter "ssh" unknown. | 参数名。 |
| 602 | issue | Documentation page seems to be down | 文档站。 |
| 615 | PR | Remove /System/Volumes/Data from fsevent paths on macOS | macOS。 |
| 621 | issue | Cannot compile with lua 5.4.1 | Lua 版本。 |
| 625 | PR | Restore rsync filter list | PR。 |
| 626 | issue | Lsyncd on MacOs 10.15 (Catalina) or later | macOS。 |
| 627 | PR | Update index.md | PR 文档。 |
| 642 | issue | Should it return the value of result instead of nil here? | PR 代码问题。 |
| 644 | PR | directory typo | PR。 |
| 646 | PR | Fix typos in index.md | PR。 |
| 649 | issue | Documentation has gone missing | 文档站。 |
| 650 | PR | implement kqueue watch method for MacOS and BSD | kqueue 平台。 |
| 655 | issue | lsyncd.1 (manpage) should go into man1? | 打包。 |
| 656 | PR | fix error - bad binary format (version mismatch) | PR runner。 |
| 661 | PR | rewrite small piece of code | PR。 |
| 664 | PR | add example for User configuration file | PR 示例。 |
| 667 | issue | Release plan | 发布计划。 |
| 673 | issue | Codespell report for "Lsyncd" (on fossies.org) | 拼写。 |
| 682 | issue | Lsyncd alternative | 讨论帖。 |
| 683 | issue | Can't build lsyncd on debian | 构建。 |
| 687 | issue | Extra steps needed to compile on Termux | Termux 平台。 |
| 688 | PR | Update FAQ entry on scripting | PR FAQ。 |
| 694 | issue | lsyncd.service exited when lsyncd run after bootup | systemd。 |
| 696 | PR | Added the 'filter' parameter to the manual | PR 文档。 |
| 697 | PR | Added the 'filter' parameter to the manual | PR 文档。 |
| 703 | PR | Update index.md | PR。 |
| 704 | PR | fix some syntax problems | PR。 |
| 705 | issue | Crontab config error | crontab 配置。 |
| 707 | issue | Error in cmake_install.cmake | 构建。 |
| 718 | issue | Alpine 3.18 lsyncd package + lua-crontab missing dependency | 打包。 |
| 722 | PR | cmake: link with dl | PR cmake。 |
| 732 | PR | Fix install of man page | PR 打包。 |
| 739 | issue | Project unmaintained? | 项目状态。 |
| 742 | PR | Make the CI run tests successfully again | PR CI。 |
| 743 | PR | Fix “bad argument #1 to '__lt'” error in lsyncd.lua | PR Lua。 |
| 745 | PR | Fix build with Lua 5.5 | PR Lua 5.5。 |

## 外部因素 / 信息不足 —— 64 条

根因在 ssh 配置、SELinux、NFS 或对象存储语义、操作事故，或报告本身不足以定位。

| # | 类型 | 标题 | 结论 |
|---|---|---|---|
| 73 | issue | rm: cannot remove '/data/test//aaaa/txt.txt': Permission denied | 目标端权限问题，非同步器缺陷。 |
| 96 | issue | ERROR: Connection to remote host failed. | SSH 连通性。 |
| 254 | issue | lsyncd terminates randomly | 标题无根因信息；gsyncd 的退出都有明确原因日志与退出码。 |
| 273 | issue | can't start lsyncd | 信息不足，需现场日志。 |
| 292 | issue | crashes in daemon mode , works with -nodaemon | 信息不足。 |
| 301 | issue | Lsyncd permission denied Error | 权限问题。 |
| 306 | issue | lsyncd and selinux | SELinux 策略。 |
| 316 | issue | Second sync only runs on launch of lsyncd | 信息不足。 |
| 320 | issue | Openvpn client cert+user/password auth | VPN 环境。 |
| 324 | issue | lsyncd stops after initial sync | 信息不足。 |
| 345 | issue | Host key verification failed | SSH 主机密钥。 |
| 353 | issue | Host key verification failed. | SSH 主机密钥。 |
| 384 | issue | Problem start lsyncd | 信息不足。 |
| 401 | issue | Lsyncd and Amazon EFS | AWS EFS 语义。 |
| 426 | issue | rsync: connection unexpectedly closed | 网络。 |
| 431 | issue | some source/targets omitted, skipped? (quietly) | 信息不足。 |
| 443 | issue | Don't  work auto sync for source 2 | 信息不足。 |
| 453 | issue | Event type problem | 信息不足。 |
| 470 | issue | lsyncd won't start-up - SELinux policy prevents this sender from sending this message to this r… | SELinux。 |
| 472 | issue | Version: 2.2.2 fails to sync | 信息不足。 |
| 474 | issue | synchronisation fail for some folders | 信息不足。 |
| 485 | issue | Permission denied after first sync | 权限。 |
| 488 | issue | Not working after reboot instance | 环境。 |
| 496 | issue | lsyncd part of the files are not synchronized | 信息不足，需现场 status_file 与日志。 |
| 497 | issue | after upgrade (from 2.1.5 repo to 2.2.3 build), service starts but nothing syncs | 信息不足。 |
| 506 | issue | Lsync runned but not sync files | 信息不足。 |
| 516 | issue | Lsyncd can't move a directory includes backslash | 信息不足。 |
| 519 | issue | startup failed when set init true | 信息不足。 |
| 533 | issue | syncd: [receiver] internal abbrev error on appdata_ocnudqtqmhau/preview/296742 (security.selinu… | SELinux xattr。 |
| 540 | issue | update files restart app | 信息不足。 |
| 543 | issue | client links dir was not  lsyncd server synchronization ? | 信息不足。 |
| 556 | issue | lsyncd with encfs | encfs 语义。 |
| 562 | issue | lsyncd just sync files when it is started | 信息不足。 |
| 564 | issue | Critical rsyncssh failure | 信息不足。 |
| 572 | issue | lsyncd fails to start | 信息不足。 |
| 585 | issue | lsyncd does not start when source dir is on glusterfs | glusterfs 语义。 |
| 590 | issue | unable to start lsyncd service | 信息不足。 |
| 591 | issue | lsyncd is terminated after running for a while. | 信息不足。 |
| 592 | issue | Lsyncd runs but does not do anything | 信息不足。 |
| 605 | issue | Question - details on remote use | 信息不足。 |
| 609 | issue | lsyncd stops after first sync | 信息不足。 |
| 610 | issue | live syncing issue from azure files | azure files 语义。 |
| 612 | issue | No route to host | 网络。 |
| 616 | issue | Tried to sync new files from remote to local, but lsyncd erased all my new files from the remot… | 操作事故（源被覆盖），非同步器缺陷；gsyncd 拒绝在源不存在时同步。 |
| 617 | issue | rsync.ssh not updating "updated" files on remote server | 信息不足。 |
| 631 | issue | The startup sync works but after that Lsyncd doesn’t do anything when I modify src files | 信息不足。 |
| 637 | issue | lsyncd: Permission denied, please try again. | SSH 认证。 |
| 638 | issue | lsync not recreating a deleted file or folder on target | 信息不足。 |
| 643 | issue | Error: error loading /etc/lsyncd/lsyncd.conf.lua | 信息不足。 |
| 647 | issue | Broken synchronisation | 信息不足。 |
| 651 | issue | Daemon startup failed | 信息不足。 |
| 652 | issue | Lsyncd failed to delete at destination server | 信息不足。 |
| 657 | issue | uploading to s3: surrogates not allowed | s3 文件名编码。 |
| 663 | issue | lsync running but not sync files | 信息不足。 |
| 668 | issue | home directory deleted | 操作事故。 |
| 670 | issue | rsync over ssh without passwordless ssh v2.2.2 | SSH 配置。 |
| 675 | issue | Failure with Create on error when syncing file with S3 | s3 后端未实现（仅 direct/rsync/rsyncssh）。 |
| 680 | issue | lsyncd with csync2 doesn't sync automatically | csync2 组合。 |
| 681 | issue | Lsync ignores executable files over ssh | 信息不足。 |
| 684 | issue | Can the lsync implement the data real-time sync of different NFS mount dir? | NFS 语义。 |
| 700 | issue | lsyncd ignoring sqlite database with rsyncssh | 信息不足（sqlite 应与普通文件无异）。 |
| 710 | issue | rsync + nfs + appledouble | 平台。 |
| 721 | issue | some files won't be synced | 信息不足。 |
| 733 | issue | lsyncd启动报错 | 信息不足。 |

## 无有效技术内容 —— 6 条

提问、讨论、重复，或已被更具体的条目覆盖。

| # | 类型 | 标题 | 结论 |
|---|---|---|---|
| 14 | issue | Project abandoned? | 项目状态讨论，非缺陷。 |
| 15 | issue | it is bug? | 信息不足。 |
| 39 | issue | Feature-Request | 空功能请求。 |
| 48 | issue | sync recursive  issue | 描述不清。 |
| 112 | issue | inode variable | 信息不足。 |
| 223 | issue | Use lsyncd to be the Guard of Web Servers | 设想，非缺陷。 |

## 验证锚点：机制 → 用例

FIX 一节按 issue 组织，同一套机制往往覆盖十几条 issue；这张表反过来按机制组织，读它是为了回答「这句话谁替我保证」。真机脚本与实测数字见 DESIGN.md 第 6 节。

| 机制 | 守护它的用例 |
|---|---|
| 同路径多次写折叠成一条 | TestManyWritesToOneEntry |
| delete+create 合并成 write、create+delete 等于没发生 | TestCollapseDeleteThenCreateIsWrite / TestCollapseCreateDeleteVanishes |
| 目录删除吸收整棵 pending 子树（批删不做二次方） | TestDirDeleteDropsChildren / TestMassDeleteOfNestedTreeDeletesEveryEntry / TestBurstDeleteOfHundredThousand |
| 重命名重定向 pending 项，不降级成 delete+create | TestRenameRetargetsPendingDelete / TestRenameDropsPendingCreates / TestMoveKeepsDirectoryKind / TestMoveReplacesSource |
| 窗口上限只置 urgent，不丢事件 | TestSizeCapFlushesEarly / TestSizeCapIsUrgentNotOverflow |
| 溢出按「次」计数 | TestOverflowCountsEpisodesNotEvents / TestOverflowEventSetsFlag |
| 失败批次回填窗口 + 指数退避 | TestFailedBatchIsReadded / TestEngineReaddsFailedBatch / TestFailedRoundBacksOff / TestBackoffGrowsAndCaps |
| 初始同步失败转成一次全量重试 | TestFailedStartupArmsFullResync / TestFailedFullResyncStaysFull |
| delay 是静默期语义，max_hold 兜底持续写入 | TestEngineRespectsDelay / TestMaxHoldFlushesAFileThatIsNeverQuiet / TestMaxHoldDefaultsAndOptOut |
| 全局与每 sync 的并发闸 | TestPerSyncTransferCapBlocks / TestNoPerSyncCapUsesGlobalOnly |
| 多 root 最长前缀路由、跨 root 移动拆成删+建 | TestNestedRootPrefersLongestPrefix / TestRoutesToCorrectRoot / TestCrossRootMoveSplitsIntoDeleteAndCreate |
| 增量批次永不带 --delete，路径走 stdin | TestIncrementalArgsNeverDelete / TestWithoutDeleteFlags / TestNulReader |
| 排除参数倒序传给 rsync | TestExcludesForwardedInReverseOrder |
| 引擎侧排除在进窗口前生效 | TestEngineFiltersExcluded / TestExcludesAreApplied |
| 退出码 23/24 视为已送达，1 仍致命 | TestBenignExitCodes / TestApplyTreatsExit24AsDelivered / TestApplyKeepsExit1Fatal / TestExitCodeUnwraps |
| 日志节流合并 + 轮转有界 | TestThrottleCollapsesWithinWindow / TestThrottleReportsSuppressedCount / TestRotationKeepsBoundedFiles / TestBufferedLinesSurviveCloseAndRotation |
| status 原子替换，不残留临时文件 | TestFlushIsAtomic / TestNoTempLeftover |
| 断点续扫：命中即跳过，失效即重核 | TestCheckpointSecondRunCopiesNothing / TestCheckpointDetectsModifiedFile / TestCheckpointDetectsFileDeletedWhileDown / TestCheckpointDetectsFileAddedWhileDown / TestCheckpointDetectsTargetTampering / TestCheckpointIgnoresForeignFingerprint / TestCheckpointCorruptFileStillMirrors / TestCheckpointUnwritableStillMirrors |
| 检查点半个段仍可用，撕裂尾只丢尾部 | TestStoreIncompleteRunIsStillUsable / TestStoreTornTailKeepsEarlierRecords / TestStoreRoundTrip |
| watch 上限降级为非致命，不中断遍历 | TestWalkAddDegradesOnWatchLimit / TestWalkAddPropagatesRealErrors |
| 新建目录立即补发已有文件 | TestAdoptBackfillsExistingContent / TestRecreatedSubtreeStillReportsEvents |
| 重命名后簿记（索引）跟着走 | TestRenameKeepsWatchBookkeeping / TestRenameRebuildsIndex / TestIndexMatchesReference / TestRekeyFollowsRenameOnRealKernel |
| 重命名告知后端「哪些路径跟着搬」 | TestRenameTellsBackendWhichPathsMoved / TestRenameWithoutRecordedSubtreeSkipsBackend |
| root 被替换或消失后重挂 | TestVerifyDetectsRecreatedRoot / TestRootGoneMarksRootLost / TestRootRemovalReportsRootGone / TestReattachRewatchesTree / TestReattachReplacesStaleChildren |
| 越界路径拒绝，源根缺失拒绝同步 | TestDirectRejectsPathsOutsideSource / TestStartupRefusesMissingSourceRoot / TestFullBatchRefusesMissingSourceRoot |
| 拷贝走 staging，不留临时文件 | TestDirectApplyNoTempLeftovers / TestSweepTempRemovesOnlyStale / TestSweepTempHonoursCancel |
| 有 write 事件必拷（同 tick 也一样） | TestSameTickWriteStillCopies |
| 只有 attrib 且内容一致时可跳过 | TestAttribOnlySkipsIdenticalFile |
| rename 回退成拷贝时仍守排除规则 | TestRenameFallbackCopyHonoursIgnore |
| 重命名把旧名字排进删除 | TestRenameSchedulesOldNameForDeletion |
| pid 文件：拒绝覆盖活实例、清理陈旧文件、建目录 | TestWritePidFileRejectsLiveInstance / TestWritePidFileReplacesStale / TestWritePidFileCreatesDirectory |
| delete = true / startup / running 三种语义真正分离 | TestDeleteModes / TestDeleteModeStartupIgnoresLaterDeletes / TestDeleteModeRunningKeepsTargetOnStartup |
| 配置拒绝未知键与源/目标重叠 | TestParseRejectsUnknownKey / TestValidateRejectsOverlappingPaths / TestShippedExampleParses / TestDocumentedKeysParse |
| 遍历顺序与 filepath.Walk 完全一致（否则检查点失效） | TestWalkOrderedMatchesFilepathWalk / TestWalkOrderedSerialFallback / TestWalkOrderedSkipDir |
| 预读按条目限流，不把树读进内存 | TestPrefetchStopsAtBudget |
| 有界拷贝不多读正在写入的字节 | TestCopyBoundedStopsAtN / TestCopyBoundedToleratesShortSource / TestCopyBoundedZeroLength |
| 内存预算：每目录、每待处理路径 | TestMemoryPerDirectory（GSYNCD_MEMDIRS 可调） / TestMillionPathsMemory（GSYNCD_AGGPATHS 可调） |
| nice / ioprio 对每个线程生效，内核拒绝也不致命 | TestApplyNiceCoversEveryThread / TestApplyIOIdleIsAccepted / TestApplyIsInertWithoutPolicy |

## 分类口径

一条 issue 归入哪一类，按「要让报告人满意，最小需要动什么」判断，而不是按标题里的关键词：

- FIX：需要一个不同的机制才能不复现。判定标准是能在代码里指出那个机制，并有对应用例或真机脚本。
- SCALE：只在 10T 量级下才暴露的设计问题。小数据量下不出现，所以不能靠复现单点 bug 解决。
- DONE：根因是运行时形态（Lua 解释器、动态链接、autotools）。
- CFG：已有配置键可直接表达，不动代码。
- TODO：真实需求，本轮不做，已在 DESIGN.md 第 5 节记录原因与替代方案。
- ENV / EXT / DUP：不适用于 Go 重写、根因在外部，或没有可执行的技术内容。

同一份数据还可以按状态看：closed 条目里有相当一部分是当年 workaround 之后关掉的，结论仍是 CFG；open 条目里 FIX 与 SCALE 占比更高，这些正是原项目至今没解决的问题。

