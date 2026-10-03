# Yuki 设计问题记录

本文只记录持续监控中能够归因到 Yuki 架构或实现的设计问题。外部操作、宿主环境告警、用户分析代码错误和容器瞬时资源状态不在本文范围内。

## 问题摘要

| 编号 | 严重度 | 设计问题 |
| --- | --- | --- |
| YD-01 | 高 | Web、Celery worker 与 RabbitMQ 共用单一容器，故障和重启影响面过大 |
| YD-02 | 高 | 工作流状态同步由读请求触发，且刷新路径随工作流规模增长 |
| YD-03 | 中 | 提交阶段串行执行 SSH 缓存检查，并重复构造相同 workflow 对象 |
| YD-04 | 中 | 默认固定启动 10 个 Celery prefork worker，空闲资源基线偏高 |
| YD-05 | 中 | SSH 连接池缺少 idle TTL 和可观测性 |
| YD-06 | 中 | 终态页面继续轮询并触发后端 workflow/file-status 工作 |
| YD-07 | 中 | 服务直接使用 Flask development server |
| YD-08 | 高 | SSH backend 没有把任务内存声明转换成机器级并发约束 |
| YD-09 | 中 | 失败详情缺少错误聚合和首个有效 traceback |
| YD-10 | 高 | SSH runner 只以“缓存目录非空”判断命中，无法识别缺失必需输出的不完整缓存 |
| YD-11 | 高 | workflow 终态转换缺少原子保护，可被多个 worker 重复执行并暴露不一致状态 |
| YD-12 | 高 | 下游 workflow 构造同步执行上游状态刷新与终态后处理，导致 job 长时间停在 waiting |
| YD-13 | 高 | job 缺少原子执行租约，重复提交可启动完全重叠的 workflow 并覆盖归属关系 |
| YD-14 | 高 | impression 级 kill 实际终止共享 workflow，且使用无效终态并可覆盖后来执行的 job 状态 |
| YD-15 | 高 | 残缺 impression 目录会被误判为已 deposited，固定 UUID 无法自动修复缺失 metadata |
| YD-16 | 中 | ImpView 将用户日志预览全文写入服务端 DEBUG 日志，造成日志放大和内容泄露风险 |

## YD-01：多种服务共用单一容器

`docker/entrypoint.sh` 在后台启动 RabbitMQ，随后 `exec yuki server start`；Web、Celery worker 和 RabbitMQ 共享同一容器生命周期。容器重启会同时中断 API、任务执行和消息代理，在途任务的恢复还依赖消息确认方式与任务幂等性。

建议：生产部署将 Web、worker 和 RabbitMQ 拆为独立服务；明确 SIGTERM 处理、子进程回收和足够的停止宽限期；验证在途任务能够安全重投。

## YD-02：状态同步由读请求驱动且刷新成本高

远端 workflow 完成后，本地 `results.json` 不会主动同步；客户端调用 `/status` 才会调度 `task_update_workflow_status`。监控到的实例包括：

- workflow `442a582d86e14a2aa22860e35e9ee9c9`：远端完成后约 14 分 58 秒才同步本地状态，终态刷新耗时 258.08 秒；
- workflow `b749f38cbf7342a6aceec29a175c613f`：远端完成到本地同步滞后约 5 分 47 秒；
- workflow `e9162089ec2d4c32b3f09ea25599a297`：运行期两轮刷新分别耗时 20.889 秒和 26.605 秒，并为大量 job 重复创建同一个 `SshWorkflow`。

这种设计让状态新鲜度取决于是否有人读取，同时令查询流量触发 O(job 数量) 的后台工作。当前冷却机制只能抑制并发，不能消除重复完整扫描；日志中的 `already pending` 还可能把冷却状态误写成已有任务在队列中。

workflow `d511dc62f39f4e56aeed9d17a52a2e17` 运行期间，多个 task 页面在约 2 秒内查询同一个 workflow。冷却锁只允许一次状态刷新，其余请求记录 `workflow refresh already pending`，但请求路径仍反复创建 `SshWorkflow`；容器 CPU 瞬时达到 52.85%，随后连续采样回落至 0.89% 和 0.69%。这说明按 task 独立轮询会对共享 workflow 形成请求放大，即使刷新任务本身已去重。

workflow `e150fc6b2cc54afe81bb7af9599541bb` 含 166 个 jobs。一次 `task_update_workflow_status` 刷新耗时 470.604 秒；刷新未完成时，页面第一轮在约 16 秒内对同一 workflow 发出 140 次状态请求，约 30 秒后又开始下一轮，均记录 `workflow refresh already pending`。后续刷新在远端已报告 50/166 后仍耗时 261.605 秒，本地进度在整轮同步完成前一直停留于 0/166，完成后才一次性跳到 50/166；再下一轮仅从 50/166 增至 53/166，也耗时 240.286 秒。远端从 53/166 推进到 154/166 的一轮同步耗时进一步增至 703.167 秒，本地在此期间保持 53/166，完成后才跳到 154/166；仅约 21 秒后页面又触发下一轮刷新，而远端已经完成 166/166。同期 CPU 在刷新期间通常约为 17%–27%，单点峰值 67.55%，完成后降至低个位数。少量增量仍接近全量刷新耗时，大批量增量则超过 11 分钟，说明刷新路径没有按变化项增量处理；冷却锁只去重远端刷新任务，没有消除按 task 重复执行的 HTTP、数据库查询和状态传播成本，而且长刷新会令本地状态持续陈旧。

workflow `e7970100a51c447f93aa38b18b7fa7c6` 的一次刷新进一步暴露了重复对象构造的具体调用链：一个 `/status` 请求在 10:22:50 调度 `task_update_workflow_status`，worker 于 10:22:58 已读到远端 `failed, 48/90`，随后进入 `_refresh_job_filelists()`。该函数逐个处理 90 个可执行 job；每个 `ImpressionStorage.refresh_filelists()` 又通过 `_get_runner_contexts()` 调用一次 `VWorkflow.create()`，即使调用方已经把同一个 workflow 对象作为参数传入。日志因此约每 1–2 秒打印一次完全相同的 `VWorkflow.create ... uuid=e7970100...`，单轮最多接近 job 数量，并为每个 job 串行查询 stageout 与 logs。除了实际开销，这条高频内部事件还以 `INFO` 级别输出，显著放大日志噪声。

建议：使用周期任务或远端完成事件主动同步；将 workflow 状态读取与输出分发扫描解耦；一次刷新内复用已经传入的 backend/workflow 对象并批量列出文件，避免 `ImpressionStorage` 再按 job 重建相同 workflow；以刷新完成时间设置最小扫描周期；区分“已入队”和“冷却期跳过”；将对象工厂的逐项诊断降为 `DEBUG` 或改为单轮聚合日志。

## YD-03：提交阶段存在串行远端往返

workflow `b749f38cbf7342a6aceec29a175c613f` 含 103 个 entries，即使远端缓存全部命中，提交仍耗时 256.917 秒。workflow `e9162089ec2d4c32b3f09ea25599a297` 构建 64 个 jobs，提交耗时 130.632 秒：约 48 秒用于逐项检查 36 个依赖，并反复构造同一个旧 workflow；约 58 秒用于约 30 次串行 SSH 缓存命中检查。

workflow `e150fc6b2cc54afe81bb7af9599541bb` 的一次批量提交包含 140 个目标 impressions，加上依赖后生成 166 个 jobs；`task_exec_impression` 从接收到 `submit finished` 共耗时 200.282 秒。构建期间日志再次显示相同终态依赖 workflow 被反复构造和逐项检查，说明提交延迟会随目标与依赖数量显著放大。

建议：按 workflow ID 对依赖分组并只读取一次状态；批量查询远端文件元数据，或对独立检查设置受控并发；复用单次 SFTP 会话，减少逐项命令启动。

## YD-04：固定 worker 数造成较高空闲基线

重启前容器空闲内存长期约 1.25 GiB。进程快照中包括 10 个 Celery prefork 子进程、Web Python 和 RabbitMQ BEAM。重启后内存一度降至约 512–578 MiB；完成多轮 workflow 后，空闲内存稳定在约 702 MiB，没有随任务结束回到启动后的低点。前后进程快照显示，这段约 24 MiB 的后期增量主要集中在 Web 主进程（约 16.6 MiB）和 RabbitMQ（约 5.8 MiB），而 Celery worker RSS 基本不变，说明大量内存属于常驻服务堆和缓存，而不是仍在执行的计算任务。

建议：使 Celery 并发数可配置；开发环境采用更低默认值；将 Web、worker 和 broker 分容器部署；以长期 cgroup `anon` 趋势判断泄漏，避免简单累加含共享页的 RSS。

## YD-05：SSH 连接池缺少空闲回收

`Yuki/kernel/ssh_pool.py` 的连接池上限为 4，但没有本地 idle TTL。访问 workflow 和 file-status 接口后，Paramiko SSH 连接及线程会保持存在，通常只在连接失效、被丢弃或进程退出时关闭。当前没有观察到无界增长，但多 runner 场景下可能按目标长期占用连接。

建议：增加可配置 idle TTL、周期清理、连接数和空闲时长指标，并明确池上限是全局还是按远端目标计算。

## YD-06：终态页面仍触发不必要的后端工作

已完成或失败的任务仍被页面约每 20–30 秒调用 `/status`、`/workflow` 和 `/file-status`。终态检测能够阻止新的状态刷新任务入队，但其余请求仍会创建 `VWorkflow`、建立 SSH/SFTP 会话并查询文件。workflow `2c9be7ce5f9b4054a66ae97fdc76eb84` 完成后，一轮页面访问在约 4 秒内至少重复创建了 14 次相同的 `SshWorkflow`，说明终态轮询仍会放大对象构造和远端访问开销。

workflow `e7970100a51c447f93aa38b18b7fa7c6` 进入 `failed` 终态后仍出现更密集的请求放大：10:29:49 起，客户端在不足 1 秒内逐个查询同一 workflow 下的多个 impression；每个 `/status` 请求虽然正确拒绝再次调度刷新，却仍重复输出 `The status is: failed` 和 `not scheduling ... already terminal (failed)`。下一批轮询约 4 秒后再次出现。终态保护只避免了后台任务重复入队，没有减少按 impression 展开的 HTTP 请求、状态读取和日志量。

建议：前端检测终态后停止轮询；服务端提供 workflow 级批量状态接口，避免页面按 impression 展开请求；服务端缓存终态 workflow 与文件清单；在同一页面请求周期内复用 `VWorkflow`；将“已是终态、未调度刷新”改为单轮聚合或采样日志。

## YD-07：使用开发服务器承载服务

启动日志明确提示正在使用 Flask development server。该服务器不适合作为生产环境的并发、超时和进程管理层。

建议：开发环境可保留现状；生产镜像使用 Gunicorn、uWSGI 等生产 WSGI server，并独立部署 worker 与 broker。

## YD-08：SSH 调度缺少全局内存额度

SSH backend 使用 `snakemake --use-conda --cores all --snakefile Snakefile`，但没有传入机器级内存资源上限。workflow `e9162089ec2d4c32b3f09ea25599a297` 中 12 个规则各声明 `kubernetes_memory_limit=8Gi`，Snakemake 将它们同时启动，声明内存合计 96 GiB。

在 SSH 本地执行模式下，每条规则的内存声明只有在 Snakemake 同时获得全局资源额度时才能约束并发；`--cores all` 只约束线程。本次执行没有发生 OOM，但该设计可能令远端宿主过量提交。

建议：从 runner 配置或远端探测获得可调度内存，并通过 Snakemake 全局 resources 传入；把规则线程数与实际 `--num-cpu` 对齐；提交时记录机器额度、规则需求和最大并发数。

## YD-09：失败信息缺少有效聚合

当多个同类任务因相同异常失败时，Yuki 会分别保存失败状态，但页面/状态摘要主要呈现异常尾部，缺少首个有效 traceback、脚本文件和行号，也没有按错误指纹聚合。在一次 12 个任务同源失败的案例中，这使错误看起来更像批量资源故障，而不是单一代码兼容错误。

建议：优先保留用户日志中的首个 traceback、脚本行号和最终异常；对相同错误指纹聚合为“一个根因、多个受影响任务”；同时保留完整原始日志供展开查看。

## YD-10：SSH 缓存命中不校验完整性

`Yuki/kernel/ssh_workflow.py` 的 `_cache_hit()` 只执行“目录存在且 `ls -A` 非空”的检查。命中后，`ContainerJob.setup_commands()` 使用通配符把缓存中现有条目链接到 workflow 的 `stageout`，不会依据预期输出清单验证每个必需文件。

监控中，workflow `ab7ebddb617a491fb3ed2e2dabe6d02e` 复用了 12 个已标记完成的选择任务；Yuki 接受远端缓存命中，但其中 11 个下游任务随后因必需的 `selected/stageout/mc.root` 不存在而失败，仅 1 个完成。重新生成这些输入缓存后，同一批 11 个任务在 workflow `6b5fde1256ea4131a48ce2f0f5b5e234` 中全部完成（11/11）。这一对照证明“目录非空”不足以代表 impression 缓存可用，缺失文件直到计算阶段才被发现。

workflow `678a2bf8b3cd4e31869291128f07b40a` 又复现了更直接的失配：输入 impression `6c611f9d12a66b8e6b8da6f6f4fbecf0` 在 Yuki 中为 `finished`，保存的 stageout 清单还记录了 `output.root`；但只读 SSH 检查确认 `pkufarm212` 上对应缓存目录已经不存在。Yuki 仍生成 `ln -s /.../6c611f9.../* imp6c611f9/stageout/` 并让 `setup.done` 成功，随后 `tmva_apply` 才以 `RDataFrame: could not open file "tmva_prep/stageout/output.root"` 失败，整个 workflow 为 `failed, 0/12`。这说明 setup 不仅没有核对 manifest，连缓存源目录和展开后的文件集合也没有可靠验证；通配符链接会把缺失缓存推迟成下游运行时错误。

源码和日志也解释了为何 SSH 的“自动缓存”没有修复该缺口：`SshWorkflow._upload_files_remote()` 在目标 runner cache 未命中后，只尝试从 Yuki 容器本地的 `Storage/<project>/<impression>/<machine>/stageout` 上传；它不会从该 impression 的上游远端 workflow workspace 复制。此次本地只有 `stageout.filelist.json` 元数据而没有实际 `stageout/` 目录，`os.path.exists(src_stageout)` 为假后代码静默跳过，随后仍上传 Snakefile 并启动执行。该 workflow 的上传日志只有 impression `29c53033...` 的一次 `Cache hit`，没有 `6c611f9...` 的 `Cache hit` 或 `Cached input`。之后显式执行 `task_cache_results` 才从上游 workspace 恢复缓存，远端复查得到 `output.root`（490669 bytes）。因此当前实现不是“缺失时自动从结果来源缓存”，而只是“目标未命中时，若本地恰有已下载实体文件则上传”。

`cache_on_runner` 本身还存在根任务与传递依赖之间的语义断层。提交 `e7970100...` 时，请求中的 `cache_on_runner` 字典只包含 12 个用户直接提交的顶层 study impression，不包含构图后加入的 `6c611f9...` 等上游生产任务。`ContainerJob._cache_commands()` 只在当前 job 为非 input 且其 runner 级配置中的 `cache_on_runner` 为真时生成 `mkdir/cp/chmod`；因此旧 workflow 中 `6c611f9...` 虽实际执行了 `prepare_tmva.py`，对应 Snakefile rule 却没有缓存 `cp`。到 workflow `678a2bf8...` 中它变成 input 后，`_cache_commands()` 又因 `self.is_input` 直接返回空列表，setup 只假设缓存已经存在。这形成了“传递依赖执行时未缓存，后续作为 SSH input 时却被无条件视为已缓存”的闭环缺口。

缓存写入也不支持安全重算或覆盖。`ContainerJob._cache_commands()` 直接执行 `mkdir -p <cache> && cp -r stageout/* <cache> && chmod -R a-w <cache>*`：首次缓存后文件被设为只读，但下一次运行同一 impression 时既不会识别已有完整缓存并跳过，也不会先创建新的临时版本再原子替换旧缓存。workflow `67aff3d81c044e588b44f67cc7298a4e` 的 24 个计算规则在用户命令完成并生成 `output.root` 后，全部尝试覆盖此前显式缓存且已只读的同名文件；`cp` 集体报 `Permission denied`，workflow 最终为 `failed (0/24)`。因此当前只读策略保护了消费者不修改共享缓存，却同时让合法的重算、恢复和缓存更新必然失败，并把缓存发布失败错误地计作计算失败。

建议：缓存写入采用 workflow 专属临时目录，在所有文件成功后写入完成标记和内容清单，再以受控锁或 compare-and-swap 原子发布；发布时应明确支持“复用相同内容”“拒绝冲突”或“版本化替换”，不能直接覆盖只读文件；缓存发布失败应与用户计算失败分开记录。缓存策略应在 DAG 构造后覆盖所有未来可能作为外部 input 的生产节点，而不是只标记用户直接提交的根任务；命中判断应校验完成标记、workflow/impression 身份、文件名、大小，必要时校验摘要；cache miss 时按数据位置选择可用来源（同 runner 的上游 workspace、Yuki 已下载实体或跨 runner 传输），任何来源均不可用时应阻止提交并明确报错；setup 阶段应先断言源目录存在，再按 manifest 逐项验证和链接，禁止以未验证的 `source/*` 作为完整性机制；发现不完整缓存时应隔离或重建，而不是继续复用。

## YD-11：workflow 终态转换存在并发竞态

`VWorkflow._entered_terminal_state()` 先读取 `results.json` 判断旧状态，SSH 状态更新随后才写回新的终态；这段“读取旧状态—传播 job 状态—写入 workflow 终态—刷新分发信息”没有 workflow 级锁、CAS 或持久化的一次性标记。多个 Celery worker 因而可以同时读到旧的非终态，并都认定自己负责首次终态处理。

workflow `e150fc6b2cc54afe81bb7af9599541bb` 的 166 个可执行 job 已全部在各自 `status.json` 中记录为 `finished`，workflow 摘要也为 `finished (166/166)`；但日志显示 ForkPoolWorker-8 于 10:03:25 开始终态分发刷新，在尚未完成时，ForkPoolWorker-7 又于 10:04:57 对同一 workflow 启动第二轮刷新。两轮都遍历 207 个 workflow 节点并对 166 个可执行 job 重复更新分发信息。该竞态使页面在同步过程中可能短暂看到 workflow、job 和分发信息处于不同版本，并把已完成的 job 显示为 `waiting`；同时重复占用两个 worker，令 workflow 完成后容器仍保持约 28.8% CPU 和约 799 MiB 内存。

建议：为每个 workflow 的状态推进和终态后处理设置跨 worker 的互斥或原子状态机；在同一事务/原子写中声明终态处理所有权，再由唯一 worker执行幂等的后处理；终态摘要只应在所有必需的 job 状态与元数据提交后对外可见，或明确返回 `finalizing`；为重复/恢复执行保存检查点，避免每次都全量遍历所有节点。

## YD-12：依赖等待与上游全量刷新同步耦合

`VWorkflow._wait_for_dependencies()` 在构造下游 workflow 的 Celery 任务内同步调用每个非终态上游的 `update_workflow_status()`。该调用不仅读取上游状态，还会传播所有 job 状态、刷新文件清单并执行终态分发扫描；因此一次慢上游刷新会长时间占住下游的提交 worker，使下游 job 一直显示 `Constructing the workflow: 1/3. waiting for the unfinished dependencies`，即使其直接依赖随后已经全部完成并缓存到目标 runner。

workflow `fcf5ca3d753242d2a930760efed044e5` 于 09:50:40 开始构造，包含 145 个节点和 50 个输入。它在 09:52:37 完成第一轮依赖枚举后，同步进入上游 workflow `e150fc6b2cc54afe81bb7af9599541bb` 的刷新；此后到至少 10:13 仍没有产生下一条自身日志，也没有 `results.json`。同期上游已写为 `finished (166/166)`，但仍在执行两轮重复的 207 节点终态分发刷新。下游的代表 job `6a0142c6ba77760ff2e4714038761db6` 和 `8c8f208bed612896b6035e394775d876` 因而持续停在 `prelude 1/3`。名义上的 60 次、每次间隔 10 秒并不是实际 wall-clock 超时，因为单次循环中的同步刷新没有被该等待窗口约束。

此外，依赖 workflow 的去重使用 `if workflow not in workflow_list`，但 `VWorkflow` 没有按 UUID 实现相等性；同一个 workflow 会因多个输入被构造为多个不同对象并重复加入列表。该实例的日志中，同一终态 workflow（例如 `c614a0e211324de4b682de02604d0691`）被重复检查多次，进一步放大对象构造和状态读取成本。

建议：下游构造只读取持久化的上游摘要，不应内联执行上游全量刷新或终态后处理；需要刷新时按 workflow UUID 投递独立、去重的任务，并让下游进入可恢复的 `blocked_on_dependencies` 状态；依赖集合直接按 workflow UUID 去重；超时使用绝对 deadline，并覆盖单次远端调用和后处理；上游完成事件应主动唤醒下游，而不是让 Celery worker 长时间轮询和休眠。

## YD-13：重复提交可并发执行同一批 job

Yuki 没有为 impression/job 建立跨请求、跨 worker 的原子执行租约，也没有在 workflow 提交前检查其可执行 job 集合是否与已有活动 workflow 重叠。`job.workflow_id` 是可被后续提交直接覆盖的单值字段，因此重复提交不仅会重复计算，还会令较早 workflow 脱离 job 页面所能追踪的归属关系。

监控中，workflow `fcf5ca3d753242d2a930760efed044e5` 因 YD-12 阻塞后，又出现 workflow `e7970100a51c447f93aa38b18b7fa7c6`。两者均包含 145 个节点和 90 个可执行 task，90/90 的可执行 job 完全相同，目标 runner 均为 `pkufarm212`。前者于 10:17:16、后者于 10:18:09 分别成功启动远端 Snakemake。共享 job（例如 `8c8f208bed612896b6035e394775d876`）的 `workflow_id` 已指向后启动的 `e7970100...`，使先启动的 `fcf5ca3d...` 成为仍在消耗远端资源、但无法通过这些 job 的当前归属字段发现的孤儿执行。

重复执行随后产生了确定的写入冲突：较早 workflow 以 `exit=0` 成功结束并生成 147 个 `.done` 标记；较新 workflow 只生成 104 个标记后以 `exit=1` 失败。其日志显示多条 `cp: ... Permission denied`：第一份执行已把共享 impression cache 中的输出复制完成并通过 `chmod -R a-w` 设为只读，第二份执行又向完全相同的 cache 路径复制同名文件而失败。本地此时仍将两份 workflow 都显示为 `running`；较新的摘要为 `0/90`，而共享 job 状态已经混合为 45 个 `finished`、3 个 `running` 和 42 个 `prelude`，证明 workflow 摘要、job 状态和远端真实结果已经分叉。

建议：提交入口用数据库唯一约束或分布式锁按 `(project, impression)` 原子获取执行租约；在创建 workflow 前拒绝或合并与活动 workflow 重叠的 job 集合；`workflow_id` 使用 compare-and-set 并保留执行历史，而不是无条件覆盖；远端启动前再次验证租约所有权；重复请求返回已有 workflow ID；终止、完成和超时路径必须可靠释放租约，并提供孤儿 workflow 检测与回收。

## YD-14：impression 级 kill 与共享 workflow 状态不一致

`GET /kill/<project>/<impression>` 通过 `ImpressionStorage.kill()` 找到该 impression 记录的 workflow，但实际调用的是整个 `workflow.kill()`；一个 workflow 中任意 impression 的 kill 操作都会终止整份 workflow，而接口名称和粒度没有体现这一影响范围。多个 impression 指向同一 workflow 时，服务端也不按 workflow UUID 去重或判断是否已经终态。

12:10:00 至 12:10:13，页面依次对多个 impression 调用 `/kill`，Yuki 因而多次向同一个远端进程组 `2868816` 发送 SIGTERM，涉及已经失败的 workflow `e7970100a51c447f93aa38b18b7fa7c6` 和 `678a2bf8b3cd4e31869291128f07b40a`。`SshWorkflow.kill()` 无论旧状态如何都会把 workflow 摘要写为字符串 `killed`，但 `status_constants.py` 的合法/终态集合只包含 `stopped`，不包含 `killed`。因此随后读取 impression 状态时，Yuki 把这个已终止 workflow 误判为非终态，又调度 `task_update_workflow_status`、重新查询远端并得到原来的 `failed, 48/90`，同时再次执行按 job 的状态传播和文件扫描。

workflow `7e272f4ac03e46429dae594e578c0427` 展示了更严重的状态反转。它在 10:43 因三个 job 的 `object_type` 为空而未能生成 Snakefile，远端计算从未启动；12:08 对其连续执行 kill 时日志反复明确记录 `No PID marker found; cannot kill remote process`，但 Yuki 仍将 workflow 摘要写为 `killed`。12:37 页面读取关联 impression 后，因为 `killed` 不属于终态而再次投递状态刷新；刷新在没有可证明存活的 PID、Snakefile 或已启动执行的情况下，又把摘要覆盖成 `running (0/12)`。也就是说，kill 既会在终止失败时宣称已终止，后续探测又会把一个从未启动的 workflow 复活为运行中，状态机缺少“远端执行身份/启动证据”这一基本不变量。

普通 `SshWorkflow.kill()` 还遍历 workflow 中全部非 input task 并直接写为 `failed`，没有像 `workflow_kill.kill_running_workflows()` 那样只保留 `job.workflow_id() == 当前 workflow UUID` 的归属检查。旧 workflow 与后来重交共享 job 时，kill 因而可能覆盖已转交给新 workflow 的 job 状态。监控中，连续 kill 后大量 job 被写为 `failed`，下游 workflow `1e14a0e8b2ec4701b960a0eb9bdab95d` 随即在构造阶段检测到 8 个失败 input 并 fail-fast，未生成 Snakefile。

14:48:52 至 14:49:00 又出现了终态 workflow 被破坏的直接证据：workflow `60644a6cb792478aa825cebd2566fbfc` 此前已经成功完成 `20/20`，但页面分别对其中的 `joint_2d_fit` 和 `plot_bs_dphi_yields` impression 调用 `/kill` 后，Yuki 仍两次向同一旧进程组 `3377658` 发送 SIGTERM，并将共享 workflow 中 21 个 job 写成 `failed`。落盘的 `results.json` 随后形成自相矛盾状态：`status="killed"`，同时仍保留 `completed=20, total=20`。紧接着提交的新任务 `28daed8b9ca85e8922f64f9c401c1005` 在 workflow `52af321209804260bbb25cb078ed63c7` 中因 18 个原已完成上游被污染为 `failed` 而立即 fail-fast。这证明问题不仅影响正在执行或已经失败的 workflow，也允许 kill 追溯性地撤销成功终态并阻断全新的下游任务。

建议：取消 impression 级直接 kill，或先解析并明确展示将受影响的整个 workflow；以 workflow UUID 为操作和幂等键，对终态 workflow 返回 no-op；统一使用合法的 `STOPPED` 状态，禁止写入未注册的 `killed`；只有确认存在与该 workflow UUID 匹配的 PID/执行标识时才允许远端状态覆盖本地状态，未生成或未启动的 workflow 不得被推断为 running；kill 未找到执行对象时应返回明确的 no-op/失败结果，不能写成已终止。终止前后均按 `job.workflow_id() == workflow.uuid` 做所有权校验，使用 compare-and-set 避免旧 workflow 覆盖后来执行；多个 impression 的批量操作必须按 workflow UUID 去重，并记录单次审计事件而不是重复发送信号。

## YD-15：残缺 impression 被误判为已 deposited，无法按原 UUID 自愈

`Job object type is empty` 并不是 task 配置中显式保存了空的 `object_type`。对受影响的五个 `Charmless/study` impression 检查后发现，其 Yuki `Storage/<project>/<impression>/` 目录中没有权威的 `config.json`，也没有完整的 `contents/`；目录只剩 `status.json`、`distribution.json`，部分还带 runner 子目录。状态接口使用缺省值读取不存在的 `config.json`，于是把元数据缺失表现成 `object_type=''`，直到 workflow 构造阶段才报错。

14:52:14，impression `fe52618bfa546e62a01d025bf25e60c4` 再次复现同一问题：`/status` 返回 `object_type=''`、空路径和空依赖；直接检查其存储目录发现目录确实存在，但唯一文件是 `distribution.json`，没有任何权威配置或内容文件。这说明仅一次分发状态写入就足以制造会被后续查询识别为“已存在 impression”的墓碑。

这一残缺状态会被接口契约永久化。`POST /purge` 会递归删除整个 impression 目录，但 `/set-impression-status` 会无条件重新创建其父目录并写入 `status.json`；其他分发状态更新也可留下 bookkeeping 文件。与此同时，`GET /deposited/<project>/<impression>` 只检查目录是否存在，不校验 `config.json`、`contents/celebi.yaml` 或上传完成标记。因此 Celebi 按相同内容生成原 UUID 并重新提交时，Yuki 会把这个只含状态文件的“墓碑目录”回答为已经 deposited，跳过完整 metadata 重传，之后每次仍复用同一个损坏 UUID并报 `empty`。

通过增加 `metadata_revision=1` 生成新 impression 只能绕开损坏记录；它改变了内容身份，掩盖了 Yuki 存储不完整且不能自愈的问题，不是正确修复。

建议：impression 上传使用临时目录并在 `config.json`、`contents/celebi.yaml` 等必需文件落盘和校验完成后原子发布，同时写入明确的完成标记；`/deposited` 必须验证完整性而非目录存在性，发现残缺目录时返回未 deposited 或专门的 corrupt 状态，并允许相同 UUID 重传修复；状态与分发写入不得凭空创建未注册 impression，至少应拒绝缺失权威 metadata 的目标；purge 与并发状态写入需加代际标识或锁，避免删除后旧 workflow 回写形成墓碑；提供按原 UUID 原地校验、隔离和修复的管理接口。

## YD-16：ImpView 把用户日志预览内容写入服务端日志

访问 `/imp-view/<project>/<impression>` 时，`generate_text_preview()` 会读取 `.txt`、`.log` 和 `.stdout` 文件；小文件保留全文，大文件保留首尾各 1000 个字符。随后 `impview()` 对包含这些预览内容的整个 `file_infos_dict` 调用 `_debug.debug(file_infos_dict)`。监控中，一次查看结果就把 `celebi_user_step0.log` 的完整内容连同分析选择、输入路径和计数结果序列化进 Docker 日志。批量查看多个 impression 时，该行为重复发生，既显著放大日志，也让原本只属于任务输出的内容进入集中服务日志及其下游采集系统。

建议：删除对完整 `file_infos_dict` 的日志输出；调试时只记录 impression、文件数量、文件名、类型和预览长度，不记录 `content` 字段。对用户日志、命令输出和配置内容采用默认脱敏策略，并为结构化日志设置字段白名单和单条大小上限。
