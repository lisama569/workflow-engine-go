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
  engine_test.go        # 提交校验 / 调度 / 重试 / 依赖 / 中断续跑 / 幂等
  timeout_test.go       # 任务超时与工作流总预算
  late_result_test.go   # 超时后迟到返回的隔离语义
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

- `SUCCEEDED / FAILED / SKIPPED / CANCELLED` 是**终态**，永不被再次执行或回退
  （`CANCELLED` 仅由工作流预算耗尽产生，见“工作流总预算语义”）。
- 工作流聚合状态：`SUBMITTED → RUNNING → SUCCEEDED|FAILED|DEADLINE_EXCEEDED`；
  任一任务 `FAILED/SKIPPED` 则工作流 `FAILED`，预算耗尽则 `DEADLINE_EXCEEDED`，
  否则 `SUCCEEDED`。

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

## 任务超时语义

- `TaskSpec.Timeout > 0` 声明**单次尝试**的最长执行时间；`<=0`（默认）不限时，行为与之前完全一致。
- 超时即失败：协调器为每次尝试派生 `context.WithTimeout` 的 ctx 传给任务函数，
  到期后本次尝试按失败判定，走与业务失败完全相同的重试/退避/耗尽路径；
  同时 ctx 被取消，任务函数可感知并尽快退出（调度侧不会等它自然返回，
  其迟到回报会被识别为过期结果丢弃，不会重复计数或迁移状态）。
- 可区分性：超时失败产生 `ATTEMPT_TIMEOUT` 事件（而非 `ATTEMPT_FAILED`），
  `LastError` 含哨兵 `ErrTaskTimeout`；重试耗尽时 `RETRIES_EXHAUSTED` 事件详情
  标注 `timed out`。业务失败（Fn 返回 error）仍走 `ATTEMPT_FAILED`，两者在事件流中明确分开。
- 超时与重试的边界：超时消耗一次尝试（与业务失败同等计次）；`MaxRetries` 用尽可能
  进入终态 `FAILED`。超时**不会**让任务被跳过或静默成功。

### 超时后的迟到回报（late result）

任务没及时响应取消、在超时判定之后才返回时，其回报称为**迟到回报**。边界规则：

- **判定以超时边界为准**：一次尝试被判超时后，该尝试的迟到回报（无论返回成功
  还是失败）一律丢弃——不算到后续重试头上，不重复扣减在跑计数，不迁移任何状态。
  本次尝试按超时失败处理，照常走重试/退避；重试那次成功还是失败只由重试自己的
  回报决定。任务退出得早还是晚，最终状态、尝试次数与事件序列完全一致。
- **实现机制**：每次派发的回报携带尝试序号，协调器只接受“当前在跑的这次尝试”的
  回报；序号或状态不匹配的回报直接丢弃。回报被协调器接收时若该尝试已越过超时
  边界（`ctx` 已 `DeadlineExceeded`），同样按超时处理——结论只取决于超时边界，
  与 select 就绪顺序无关，同一用例反复跑结论不漂。
- **不会提前收场、不会翻回终态**：迟到的成功不会把任务/工作流提前推进到终态，
  也不会把还在跑的重试标记为已完成；已进入终态（含重试耗尽后的 `FAILED`）的
  任务，迟到回报不能将其翻回。旧运行被中断后，其迟到回报随运行 ctx 取消被丢弃，
  不会泄漏进续跑。
- **与幂等叠加**：并发/重复触发仍共享同一次执行，迟到回报不会导致额外派发，
  结论唯一。
- **不受影响的场景**：未声明 `Timeout` 的任务没有“迟到”概念，行为完全不变；
  正常配合取消（ctx 取消后尽快返回）的任务，其返回发生在超时判定之后、
  状态已离开 `RUNNING`，同样被丢弃，语义与之前一致；退避长短（含无退避）
  不改变上述结论。

## 工作流总预算语义

- `WithBudget(d)` 声明整个工作流的总时长预算；`d<=0`（默认）无预算。
- 预算从**第一次触发**算起：首次触发时把 `Deadline = 首次触发时刻 + Budget`
  持久化到 `WorkflowState`，续跑、并发触发都沿用同一个 `Deadline`，语义前后一致。
- 预算到期后：
  - **不再派发新任务**（循环顶部与派发条件双重判定，定时器到期即唤醒收尾）；
  - **在跑任务立即取消**而非自然收尾——预算的语义就是到点收场，等待自然收尾
    可能被卡住的任务无限拖延；在跑任务的 ctx 随协调循环退出被统一取消，可感知退出；
  - 未完成任务（RUNNING/RETRYING/READY/PENDING）统一收敛到终态 **`CANCELLED`**，
    已完成任务的 `SUCCEEDED`/`FAILED` 结果原样保留；
  - 工作流落到终态 **`DEADLINE_EXCEEDED`**，与普通失败 `FAILED` 明确区分，
    事件原因 `BUDGET_EXHAUSTED`。
- 与续跑叠加：中断期间预算到期的，`Resume` 不再执行任何任务，直接把未完成
  任务收敛为 `CANCELLED` 并给出 `DEADLINE_EXCEEDED` 终态结论；该终态同样幂等，
  重复触发/续跑返回同一快照，状态不回退。
- 与幂等叠加：并发/重复触发共享同一次执行，预算到期结论唯一，不会因并发多派任务。
- 资源回收：每次尝试的超时控制复用 `context.WithTimeout`（结果到达或判定后立即
  cancel），退避定时器在中断/收尾时统一 Stop，预算定时器每次运行只有一个并随
  运行结束回收——长时间反复运行不会累积定时器或协程
  （`TestTimeoutResourcesReclaimed` 验证协程数回落到基线）。

## 中断与退避中的尝试计数

- `RUNNING` 被中断：视为同一次尝试的重新执行，续跑沿用原 `Attempt`，不多计。
- `RETRYING`（退避中）被中断：失败尝试已计数，中断视为退避结束，回收时
  `Attempt+1`，续跑执行的是下一次尝试，不少计。

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
go test -race -v ./...        # 逐用例打印名字、覆盖点（COVERAGE）与结果（RESULT）
go test -race -count=10 ./... # 竞态检测下多轮重复，验证稳定性
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
| `TestTaskTimeout_RetryThenSuccess` | 单任务超时按失败计、任务感知取消、重试后成功，事件与业务失败可区分 |
| `TestTaskTimeout_RetriesExhausted` | 连续超时耗尽重试后终态 FAILED，LastError 含 ErrTaskTimeout |
| `TestWorkflowBudgetExhausted` | 总预算到期：不再派发、在跑任务取消、未完成任务 CANCELLED、DEADLINE_EXCEEDED |
| `TestTimeoutWithInterruptAndResume` | 超时退避期间中断，续跑后尝试计数不重复、终态成功 |
| `TestBudgetExhaustedBeforeResume` | 中断期间预算到期，续跑直接落 DEADLINE_EXCEEDED，已完成任务不重跑 |
| `TestConcurrentTriggers_BudgetConclusionUnique` | 预算场景 5 路并发触发结论唯一、不多派任务 |
| `TestTimeoutAndBudgetValidation` | 负数超时/预算提交期以可区分错误拒绝 |
| `TestTimeoutResourcesReclaimed` | 20 轮高频超时后协程数回落基线，定时器/协程不累积 |
| `TestLateFailureAfterTimeout_RetrySuccessDecides` | 超时后迟到返回失败 + 重试自身成功 → SUCCEEDED，迟到失败不算到重试头上 |
| `TestLateSuccessAfterTimeout_RetryFailureDecides` | 超时后迟到返回成功 + 重试自身失败 → FAILED，迟到成功不翻盘 |
| `TestLateSuccessAfterTimeout_RetrySucceeds` | 迟到成功 + 重试也成功 → 终态来自重试自己的回报，不提前收场 |
| `TestLateSuccessAfterTimeout_RetriesExhausted` | 每次尝试都超时且迟到成功 → 额度耗尽 FAILED，终态不被翻回 |
| `TestLateResultWithInterruptAndResume` | 超时+迟到返回+退避期间中断 → 续跑重试成功，旧回报不泄漏、不多派 |
| `TestLateResult_ConcurrentTriggersUniqueConclusion` | 超时+迟到返回场景 5 路并发触发，结论唯一、任务只跑一轮 |

迟到回报相关用例的时序用 channel 精确编排（不依赖 sleep 余量）：尝试 1 卡在
不受取消影响的外部调用里，超时判定后由测试放行其迟到返回，保证迟到回报确定地
落在“重试仍在执行”的窗口内——这正是原缺陷的触发窗口。

`-v` 日志中会逐条打印任务状态迁移与触发依据（`[EVENT] REASON ... FROM -> TO`），
便于人工核对迁移序列自洽性。
