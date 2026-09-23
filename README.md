# 本地工作流编排引擎（Go）

一个纯内存状态的本地 DAG 工作流引擎，用单协调器（scheduler coordinator）+ 工作协程的模型
模拟任务依赖、并发调度、失败重试与断点续跑。所有状态迁移都经过 `Store` 写穿，并通过
`EventSink` 对外发出可解释、自洽、可复现的事件序列。

## 目录结构

```
workflow/
  errors.go   # 可区分的提交期错误（哨兵错误 + ValidationError 上下文）
  types.go    # 任务/工作流状态机、事件、重试策略等类型
  graph.go    # 提交期静态校验 + 确定性拓扑排序（Kahn）
  backoff.go  # 退避策略：NoBackoff / Constant / Linear
  store.go    # Store 抽象 + 线程安全内存实现（带深拷贝）
  engine.go   # 引擎入口：Submit / Trigger / Resume / Cancel / GetState
  run.go      # 单协调器调度循环（状态机唯一写者）
  log.go      # LogEventSink：状态迁移文本日志
  engine_test.go
```

## 快速上手

```go
engine := workflow.NewEngine(
    workflow.NewMemoryStore(),
    workflow.WithEventSink(workflow.NewLogEventSink(os.Stdout)),
)

err := engine.Submit("order-pipeline", []workflow.TaskSpec{
    {ID: "validate", Fn: validateFn},
    {ID: "charge",   Deps: []string{"validate"},
        Retry: workflow.RetryPolicy{
            MaxRetries: 3,
            Backoff:    workflow.LinearBackoff{Interval: 10 * time.Millisecond},
        },
        Fn: chargeFn},
    {ID: "ship", Deps: []string{"charge"}, Fn: shipFn},
}, workflow.WithConcurrency(2))
if err != nil { /* 提交期错误，见下文，原因可区分 */ }

wf, err := engine.Trigger(ctx, "order-pipeline") // 阻塞至终态
```

## 状态机

任务：

```
PENDING ──依赖全部 SUCCEEDED──▶ READY ──获得并发槽──▶ RUNNING
   │                              ▲   │                 │
   │依赖 FAILED/SKIPPED           │   │ 失败且还有重试额度│
   ▼                              │   ▼                 ▼
 SKIPPED                    RETRYING ◀┘          成功 SUCCEEDED
                                  │   退避到期 Attempt++
                                  └──▶ READY
                       失败且重试耗尽 ──▶ FAILED
```

- `SUCCEEDED / FAILED / SKIPPED` 是**终态**，永不被再次执行或回退。
- 工作流聚合状态：`SUBMITTED → RUNNING → SUCCEEDED|FAILED`；
  任一任务 `FAILED/SKIPPED` 则工作流 `FAILED`，否则 `SUCCEEDED`。

## 调度规则

1. **提交期拒绝（fail-fast）**：`Submit` 同步执行静态校验，不合法的图不会进入运行阶段：
   - 循环依赖（含自依赖）→ `ErrCycleDetected`
   - 依赖指向不存在的任务 → `ErrDependencyMissing`
   - 任务 ID 重复 / 空工作流 → `ErrDuplicateTask` / `ErrEmptyWorkflow`
   - 并发上限 ≤ 0 → `ErrInvalidConcurrency`；重试次数 < 0 → `ErrInvalidRetry`

   所有错误均包装为 `*ValidationError`（携带 `TaskID` 与 `Detail`），可用
   `errors.Is(err, workflow.ErrCycleDetected)` 等区分原因，用 `errors.As` 取上下文。
   因此**不会出现运行中途静默卡死**。
2. **依赖门槛**：任务仅在所有依赖都 `SUCCEEDED` 时才可运行；任一依赖终态失败，
   该任务（及其下游）级联标记为 `SKIPPED`，任务函数不会被调用。
3. **并发上限**：`WithConcurrency(n)` 限制同时处于 `RUNNING` 的任务数；
   派发顺序沿确定性拓扑序（同层按任务 ID 排序），取第一个 `READY` 任务，
   保证同一规格多次运行的结论可复现。
4. **无竞争推进**：状态迁移、依赖释放、尝试计数只发生在单一协调器协程中；
   任务函数在工作协程执行，只通过通道回报结果，从结构上杜绝漏跑/重复跑。

## 重试语义

- `RetryPolicy.MaxRetries` 表示**首次执行之外的额外尝试次数**；
  `MaxRetries=2` 时任务最多执行 3 次，`Attempt` 依次为 1、2、3。
- 一次尝试失败：
  - `Attempt <= MaxRetries`：迁移到 `RETRYING`，按 `Backoff.Next(attempt)`
    （支持 `NoBackoff` / `ConstantBackoff` / `LinearBackoff`）退避后回到 `READY`，
    `Attempt++`；
  - 否则迁移到终态 `FAILED`（原因 `RETRIES_EXHAUSTED`），`LastError` 保留
    最后一次失败信息，下游随后 `SKIPPED`。
- 退避定时器由协调器管理；`Cancel` 中断时所有定时器被回收，不会在续跑外触发。

## 断点续跑语义

- `Cancel(id)`（或引擎进程退出）中断执行时：
  - 已终态任务保持终态；
  - `RUNNING/RETRYING` 被规范化回收为可再次执行状态（内存实现中 `READY`，
    并保留 `Attempt`）；未获依赖的任务保持/回到 `PENDING`。
- `Resume(ctx, id)` 重新加载 Store 中的状态：
  - `SUCCEEDED/FAILED/SKIPPED` 任务**直接跳过，任务函数不再执行，Attempt 不变**；
  - 被回收的任务沿用中断前的 `Attempt` 重新执行（语义为“同一次尝试的重新执行”，
    中断+续跑不会多消耗一次重试额度，状态不回退）；
  - 续跑沿用提交时确定的并发上限与拓扑序。
- 交付保证：完成的任务**恰好一次生效**；被中断在执行中的任务按 **at-least-once**
  重新执行（任务函数应自行保证幂等，这是通用工作流系统的标准取舍）。

## 幂等与一致性

- 对同一工作流的**并发或重复 `Trigger` 共享同一次执行**：后来的调用不启动新调度，
  只等待同一个终态，任务函数不会因并发触发而多跑。
- 工作流已处于终态后再次 `Trigger/Resume`：直接返回同一份终态快照，
  发出 `DEDUPLICATED` 事件，不执行任何任务。
- `GetState` 与终态返回值均为 Store 深拷贝，外部持有快照不会污染内部状态。
- 每个状态迁移都发出 `Event`（`From/To/Reason/Detail`），观测序列即真实执行序列。
  触发依据包括：`DEPS_MET`、`SCHEDULED`、`ATTEMPT_FAILED`、`RETRY_BACKOFF`、
  `RETRIES_EXHAUSTED`、`DEP_FAILED`、`WF_DONE`、`RESUME`、`DEDUPLICATED`。

## 验证方法

```bash
go test -race -v ./...
```

覆盖场景：

| 测试 | 覆盖点 |
| --- | --- |
| `TestSubmit_ValidationErrors` | 空图 / 重复 ID / 缺失依赖 / 自环 / 多节点环，错误原因可区分 |
| `TestSubmit_InvalidConcurrency` | 非法并发上限在提交期拒绝 |
| `TestHappyPath_DependencyOrder` | 依赖全部成功才运行、实际执行顺序符合拓扑约束 |
| `TestConcurrencyLimit` | limit=1/2/3 下实测最大在飞任务数恰好等于上限 |
| `TestRetryExhausted` | 3 次尝试后终态 FAILED、Attempt 与 LastError 正确、终态事件唯一 |
| `TestRetryThenSuccess` | 前两次失败第三次成功，退避后恢复 |
| `TestDependencyFailure_SkipsDependents` | 上游失败级联 SKIPPED 且下游函数不执行，无关分支不受影响 |
| `TestInterruptAndResume` | 执行中 Cancel，已完成任务续跑不重复执行、Attempt 不重复计数 |
| `TestDuplicateAndConcurrentTriggers` | 5 路并发触发 + 终态后再触发，任务总执行次数唯一 |
| `TestDeterministicTerminalState` | 同规格 5 次运行终态与 Attempt 完全一致 |

`-v` 日志中会逐条打印任务状态迁移与触发依据（`[EVENT] REASON ... FROM -> TO`），
便于人工核对迁移序列自洽性。
