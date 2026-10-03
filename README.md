# 本地工作流编排引擎（Go）

一个纯内存状态的本地 DAG 工作流引擎，用单协调器（scheduler coordinator）+ 工作协程的模型
模拟任务依赖、并发调度、失败重试与断点续跑。所有状态迁移都经过 `Store` 写穿，并通过
`EventSink` 对外发出可解释、自洽、可复现的事件序列。

## 目录结构

```
workflow/
  errors.go   # 可区分的提交期错误（哨兵错误 + ValidationError 上下文）
  types.go    # 任务/工作流状态机、事件、重试策略、超时与预算等类型
  graph.go    # 提交期静态校验 + 确定性拓扑排序（Kahn）
  backoff.go  # 退避策略：NoBackoff / Constant / Linear
  store.go    # Store 抽象 + 线程安全内存实现（带深拷贝）
  engine.go   # 引擎入口：Submit / Trigger / Resume / Cancel / GetState
  run.go      # 单协调器调度循环（状态机唯一写者，含超时与预算控制）
  log.go      # LogEventSink：状态迁移文本日志
  engine_test.go    # DAG/重试/续跑/幂等用例
  timeout_test.go   # 超时与预算用例
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
        Timeout: 5 * time.Second, // 单次尝试最长执行时间，超时按失败计入并重试
        Retry: workflow.RetryPolicy{
            MaxRetries: 3,
            Backoff:    workflow.LinearBackoff{Interval: 10 * time.Millisecond},
        },
        Fn: chargeFn},
    {ID: "ship", Deps: []string{"charge"}, Fn: shipFn},
}, workflow.WithConcurrency(2),
   workflow.WithBudget(time.Minute)) // 整条流水线的总时长预算，到点收口为 CANCELLED
if err != nil { /* 提交期错误，见下文，原因可区分 */ }

wf, err := engine.Trigger(ctx, "order-pipeline") // 阻塞至终态
```

## 状态机

任务：

```
PENDING ──依赖全部 SUCCEEDED──▶ READY ──获得并发槽──▶ RUNNING
   │                              ▲   │                 │
   │依赖 FAILED/SKIPPED           │   │ 失败或超时且     │成功
   ▼                              │   │ 还有重试额度     ▼
 SKIPPED                    RETRYING ◀┘          SUCCEEDED
                                  │   退避到期 Attempt++
                                  └──▶ READY
                       失败或超时且重试耗尽 ──▶ FAILED
   工作流预算耗尽时，一切非终态任务 ──▶ CANCELLED（终态）
```

- `SUCCEEDED / FAILED / SKIPPED / CANCELLED` 是**终态**，永不被再次执行或回退。
- 工作流聚合状态：`SUBMITTED → RUNNING → SUCCEEDED|FAILED|CANCELLED`；
  任一任务 `FAILED/SKIPPED` 则工作流 `FAILED`，全部成功则 `SUCCEEDED`，
  总预算耗尽收口为 `CANCELLED`。

## 调度规则

1. **提交期拒绝（fail-fast）**：`Submit` 同步执行静态校验，不合法的图不会进入运行阶段：
   - 循环依赖（含自依赖）→ `ErrCycleDetected`
   - 依赖指向不存在的任务 → `ErrDependencyMissing`
   - 任务 ID 重复 / 空工作流 → `ErrDuplicateTask` / `ErrEmptyWorkflow`
   - 并发上限 ≤ 0 → `ErrInvalidConcurrency`；重试次数 < 0 → `ErrInvalidRetry`

   所有错误均包装为 `*ValidationError`（携带 `TaskID` 与 `Detail`），可用
   `errors.Is(err, workflow.ErrCycleDetected)` 等区分原因，用 `errors.As` 取上下文。
   因此**不会出现运行中途静默卡死**。
   超时与预算配置同样在提交期校验：任务 `Timeout < 0` → `ErrInvalidTimeout`，
   工作流 `Budget < 0` → `ErrInvalidBudget`。
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

## 任务超时语义

- `TaskSpec.Timeout` 声明**单次尝试**的最长执行时间（`0` = 不限制，行为与旧版一致）。
- 每次尝试在独立的 `context.WithTimeout` 中执行：超时即取消该尝试的 context，
  **任务函数能通过 `ctx.Done()` 感知取消并尽快退出**；同时这次尝试按失败计入，
  走与业务失败完全相同的重试/退避/耗尽路径（`Attempt` 计数、退避策略不变）。
- 超时与业务失败**可区分**：
  - 事件依据为 `ATTEMPT_TIMEOUT`（普通失败是 `ATTEMPT_FAILED`）；
  - `TaskState.LastError` 与事件 Detail 含哨兵文本 `ErrTaskTimeout`
    （`"workflow: task attempt timed out"`）；
  - 重试耗尽后终态仍是 `FAILED`（依据 `RETRIES_EXHAUSTED`），`LastError` 保留超时原因。
- 顽固任务（无视取消、超时后仍返回）的**迟到结果会被协调器按 attempt 对账丢弃**，
  不会把已落定的状态翻回；其协程在工作流收尾（runCtx 取消）后退出发送，不会滞留。
- 每次尝试的定时器在尝试结束（成功/失败/超时/收口）时立即取消释放，
  长时间反复运行不会积累定时器与协程。

## 工作流总预算语义

- `WithBudget(d)` 声明整条工作流的总时长预算。`Deadline` 在**第一次触发
  （首次运行的起点）**设定并持久化，续跑沿用同一 Deadline——即预算语义是
  “按第一次触发算起的总时长”，前后一致、可预期。
- 预算耗尽（到点）后的处理：
  - **不再派发任何新任务**；
  - **在途任务立刻取消**（而非自然收尾）。理由：预算的意义就是给流水线一个硬性
    总时长上界，若允许在途任务自然结束，一个卡住的外部调用仍会把工作流无限挂住，
    正是本特性要解决的问题。取消经 context 传递，配合的任务尽快退出；
    不配合的任务其迟到结果被对账丢弃，结论依然确定、可复现；
  - 已完成任务的结果**原样保留**；
  - 其余所有非终态任务（在途/待重试/就绪/等待依赖）一律以终态 `CANCELLED` 落定，
    `LastError` 为 `ErrBudgetExhausted`；
  - 工作流聚合为终态 `CANCELLED`，事件依据 `BUDGET_EXHAUSTED`——
    与普通失败（`FAILED`/`RETRIES_EXHAUSTED`）和用户主动 `Cancel`
    （只中断、可续跑、不产生终态）都可区分。
- 续跑时若预算早已用尽：不执行任何任务，直接把非终态任务收口为 `CANCELLED`、
  工作流落定 `CANCELLED`，立即返回终态结论。

## 与重试 / 续跑 / 幂等的边界

- **什么情况算失败**：任务函数返回 error、单次尝试超时，都计为一次失败尝试，
  消耗相同的重试额度；预算耗尽不是“失败”，是独立的 `CANCELLED` 终态。
- **什么情况不会被重跑**：`SUCCEEDED/FAILED/SKIPPED/CANCELLED` 终态任务在任何
  续跑/重复触发下都不会再执行；预算到点后不再派发新任务。
- **续跑一致性**：`Deadline` 锚定首次触发，续跑不重算预算；被中断回收的任务沿用
  原 `Attempt` 重执行（不多计重试）；已超时/已失败计入的 `Attempt` 不回退。
- **幂等触发**：并发/重复 `Trigger` 共享同一次执行，超时与预算控制只在唯一的
  协调循环内生效，不会因并发触发多派任务；工作流终态（含 `CANCELLED`）后再触发
  直接返回既有终态（`DEDUPLICATED`）。
- **资源回收**：退避定时器、每次尝试的超时定时器、预算截止定时器都在协调循环
  退出前统一回收；运行结束即取消 runCtx，被遗弃的工作协程随之退出。

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
  触发依据包括：`DEPS_MET`、`SCHEDULED`、`ATTEMPT_FAILED`、`ATTEMPT_TIMEOUT`、
  `RETRY_BACKOFF`、`RETRIES_EXHAUSTED`、`DEP_FAILED`、`WF_DONE`、`RESUME`、
  `DEDUPLICATED`、`BUDGET_EXHAUSTED`。

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
| `TestSubmit_InvalidTimeoutAndBudget` | 非法 Timeout/Budget 在提交期以可区分错误拒绝 |
| `TestTaskTimeout_RetryThenSuccess` | 首次尝试超时（任务感知取消）→ 退避重试 → 成功，ATTEMPT_TIMEOUT 可区分 |
| `TestTaskTimeout_ExhaustsRetries` | 持续超时耗尽重试 → FAILED；顽固任务迟到结果被丢弃、状态不回退 |
| `TestWorkflowBudget_Exhausted` | 预算到点：在途取消、未跑收口 CANCELLED、已完成保留、工作流终态 CANCELLED |
| `TestBudget_ResumeAfterExpiry` | 预算+中断续跑叠加：Deadline 锚定首次触发，续跑遇预算已尽直接终态、零重跑 |
| `TestTimeout_InterruptResume` | 超时退避中被 Cancel，续跑沿用 Attempt 重执行并成功 |
| `TestTimeout_ConcurrentTriggerUnique` | 超时场景 5 路并发触发共享一次执行，结论唯一 |
| `TestTimeout_NoTimerOrGoroutineLeak` | 30 批高频超时工作流后协程数回落基线，定时器/协程无积累 |

`-v` 日志中会逐条打印任务状态迁移与触发依据（`[EVENT] REASON ... FROM -> TO`），
每个用例开头打印 `COVERAGE:` 说明覆盖的业务、结尾打印 `RESULT:` 结论，
便于人工核对迁移序列自洽性。
